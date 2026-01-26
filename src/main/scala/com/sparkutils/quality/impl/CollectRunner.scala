package com.sparkutils.quality.impl

import com.sparkutils.quality._
import com.sparkutils.quality.impl.imports.RuleFolderRunnerImports
import com.sparkutils.quality.impl.util.PassThroughCompileEvals
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{CreateArray, Expression, NonSQLExpression}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.{ClassTag, classTag}


private[quality] object CollectRunnerUtils extends RuleFolderRunnerImports {

  def compiledEval[T](results: InternalRow, output: ArrayBuffer[T]): InternalRow =
    InternalRow(results, new GenericArrayData(output))

  def addOne[T](output: ArrayBuffer[T], an: T): Unit = output.+=(an)

}


/**
  * Children will be rewritten by the plan, it's then re-incorporated into ruleSuite
  * expressionOffsets.length is the length of the trigger expressions in realChildren, realChildren(expressionOffsets.length + expressionOffsets(x)) will be the correct OutputExpression
  */
trait CollectRunnerBase[T] extends Expression with NonSQLExpression {

  val ruleSuite: RuleSuite
  val resultDataType: Option[DataType]
  val variablesPerFunc: Int
  val variableFuncGroup: Int
  val expressionOffsets: Array[Int]
  val flatten: Boolean
  val includeNulls: Boolean
  val triggerCount: Int

  implicit val classTagT: ClassTag[T]
  val tClass: Class[T]

  import RuleEngineRunnerUtils._
  import RuleFolderRunnerUtils._

  lazy val actualType =
    resultDataType.getOrElse{
      children.last.dataType
    }

  // only used for compilation compatibility with ruleEngine utils code
  lazy val compiledRealChildren = Array.empty[ExpressionWrapper]

  lazy val canFlatten = actualType.isInstanceOf[ArrayType]
  lazy val elementType: DataType = if (!canFlatten) null else actualType.asInstanceOf[ArrayType].elementType

  // e.g. starter space for each rule with 5 possible rows when flattening or exact size when it's an array
  // to find out the exact size after casting etc. involves a second pass and is measurably more expensive
  lazy val starterSize = {
    val initial = ruleSuite.ruleSets.map(_.rules.size).sum

    if (flatten && canFlatten) {
      // if array
      children.foldLeft(initial) { case (cur, e) =>
         e match {
           case a: CreateArray => a.children.size + cur
           case _ => cur + 5 // number carefully picked from thin air
         }
      }
    } else
      initial
  }


  override def nullable: Boolean = false
  override def toString: String = s"${classTagT.runtimeClass.getName}(${children.mkString(", ")})"

  // used only for eval, compiled uses the children directly
  lazy val reincorporated = reincorporateExpressions(ruleSuite, children, false, expressionOffsets)

  override def eval(input: InternalRow): Any = {
    val (res, processedRes) = //(null, null)
      RuleSuiteFunctions.collect(reincorporated, input, flatten && canFlatten, includeNulls,
        elementType, starterSize)
    InternalRow(com.sparkutils.quality.impl.RuleRunnerUtils.ruleResultToRow(res), processedRes)
  }

  def dataType: DataType = StructType( Seq(
      StructField(name = "ruleSuiteResults", dataType = com.sparkutils.quality.types.ruleSuiteResultType),
      StructField(name = "result", dataType =
        if (flatten && canFlatten) ArrayType(elementType, includeNulls) else ArrayType(actualType, includeNulls),
        nullable = true)
    ))

  protected def doGenCodeI(ctx:  _root_.org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext, ev:  _root_.org.apache.spark.sql.catalyst.expressions.codegen.ExprCode): _root_.org.apache.spark.sql.catalyst.expressions.codegen.ExprCode = {
    ctx.references += this

    // needs resetting every row
    val bufferTerm = ctx.addMutableState(classOf[ArrayBuffer[_]].getName, ctx.freshName("results"))

    // order by salience
    val salience = com.sparkutils.quality.impl.RuleEngineRunnerUtils.flattenSalience(ruleSuite)
    val outputs = 0 until triggerCount
    val reordered = outputs zip salience sortBy(_._2) map(_._1)

    //val outputExprs = children.drop(expressionOffsets.length).map(_.genCode(ctx))

    val arrayData = ctx.freshName("arrayData")
    val z = ctx.freshName("z")
    val o = ctx.freshName("o")

    val compilerTerms =
      RuleEngineRunnerUtils.genCompilerTerms[T](ctx, PassThroughCompileEvals(children), expressionOffsets, children,
        false, variablesPerFunc, variableFuncGroup, false,
        // capture the current
        extraResult = (outArrTerm: String) =>
          s"""
             if (($outArrTerm != null) || $includeNulls) {
                if (($outArrTerm == null) || ${!(flatten && canFlatten)}) {
                  com.sparkutils.quality.impl.CollectRunnerUtils.addOne($bufferTerm, $outArrTerm);
                } else {
                  // flatten case and non-null
                  ArrayData $arrayData = (ArrayData) $outArrTerm;
                  for (int $z = 0; $z < $arrayData.numElements(); $z++) {
                    Object $o = ${CodeGenerator.getValue(arrayData, elementType, z)};
                    if (($o != null) || $includeNulls) {
                      com.sparkutils.quality.impl.CollectRunnerUtils.addOne($bufferTerm, $o);
                    }
                  }
                }
             }
           """,
        orderOffset = (idx: Int) => reordered(idx),
        // we shouldn't check salience as we are already ordered by it
        salienceCheck = false
      )

    import compilerTerms._

    val pre = s"""
          $currentSalience = java.lang.Integer.MAX_VALUE;
          $currentOutputIndex = -1;
          $pushToTop
          $bufferTerm = new ${classOf[ArrayBuffer[_]].getName}($starterSize);

          ${funNames.map{f => s"$f($paramsCall);"}.mkString("\n")}
      """
    val post = s"""

          boolean ${ev.isNull} = false;
      """

    val res =
      ev.copy(code = code"""
        $pre

        InternalRow ${ev.value} =
          com.sparkutils.quality.impl.CollectRunnerUtils.compiledEval(
            $utilsName.evalArray($ruleSuitTerm, $ruleSuiteArrays, $resArrTerm),
            $bufferTerm);

        $post
        """
      )

    res

  }
}

/**
 * Children will be rewritten by the plan, it's then re-incorporated into ruleSuite
 * expressionOffsets.length is the length of the trigger expressions in realChildren, realChildren(expressionOffsets.length + expressionOffsets(x)) will be the correct OutputExpression
 */
case class CollectRunnerRunner(ruleSuite: RuleSuite, children: Seq[Expression], resultDataType: Option[DataType],
                                variablesPerFunc: Int, variableFuncGroup: Int, expressionOffsets: Array[Int],
                               triggerCount: Int,
                               flatten: Boolean, includeNulls: Boolean
                               ) extends CollectRunnerBase[CollectRunnerRunner] {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = doGenCodeI(ctx, ev)

  override implicit val classTagT: ClassTag[CollectRunnerRunner] = ClassTag(classOf[CollectRunnerRunner])
  override val tClass: Class[CollectRunnerRunner] = classOf[CollectRunnerRunner]
}
