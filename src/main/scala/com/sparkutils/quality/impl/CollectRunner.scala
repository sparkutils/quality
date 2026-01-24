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

  /**
   * Needs sorting in salience order for output processing but NOT for result's.
   * So output array should use offsets, results should be offset, but order
   * of calling must be salience based.
   *
   */

  def compiledEvalDebug[T](results: InternalRow, output: T): InternalRow =
    InternalRow(results, output)

  def compiledEval[T](results: InternalRow, output: ArrayBuffer[T]): InternalRow =
    InternalRow(results, new GenericArrayData(output))


}


/**
  * Children will be rewritten by the plan, it's then re-incorporated into ruleSuite
  * expressionOffsets.length is the length of the trigger expressions in realChildren, realChildren(expressionOffsets.length + expressionOffsets(x)) will be the correct OutputExpression
  */
trait CollectRunnerBase[T] extends Expression with NonSQLExpression {

  val ruleSuite: RuleSuite
  val resultDataType: DataType
  val debugMode: Boolean
  val variablesPerFunc: Int
  val variableFuncGroup: Int
  val expressionOffsets: Array[Int]
  val flatten: Boolean
  val includeNulls: Boolean

  implicit val classTagT: ClassTag[T]
  val tClass: Class[T]

  import RuleEngineRunnerUtils._
  import RuleFolderRunnerUtils._

  // only used for compilation compatibility with ruleEngine utils code
  lazy val compiledRealChildren = Array.empty[ExpressionWrapper]

  lazy val canFlatten = resultDataType.isInstanceOf[ArrayType]
  lazy val elementType: DataType = if (!canFlatten) null else resultDataType.asInstanceOf[ArrayType].elementType

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
      RuleSuiteFunctions.collect(reincorporated, input, debugMode, flatten && canFlatten, includeNulls,
        elementType, starterSize)
    InternalRow(com.sparkutils.quality.impl.RuleRunnerUtils.ruleResultToRow(res), processedRes)
  }

  def dataType: DataType = StructType( Seq(
      StructField(name = "ruleSuiteResults", dataType = com.sparkutils.quality.types.ruleSuiteResultType),
      StructField(name = "result", dataType =
        if (flatten && canFlatten) ArrayType(elementType, includeNulls) else ArrayType(resultDataType, includeNulls),
        nullable = true)
    ))

  protected def doGenCodeI(ctx:  _root_.org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext, ev:  _root_.org.apache.spark.sql.catalyst.expressions.codegen.ExprCode): _root_.org.apache.spark.sql.catalyst.expressions.codegen.ExprCode = {
    ctx.references += this

    // needs resetting every row
    val bufferTerm = ctx.addMutableState(classOf[ArrayBuffer[_]].getName, ctx.freshName("results"))

    // order by salience
    val salience = com.sparkutils.quality.impl.RuleEngineRunnerUtils.flattenSalience(ruleSuite)
    val outputs = 0 until (children.size - expressionOffsets.size)
    val reordered = outputs zip salience sortBy(_._2) map(_._1)

    //val outputExprs = children.drop(expressionOffsets.length).map(_.genCode(ctx))

    val arrayData = ctx.freshName("arrayData")
    val z = ctx.freshName("z")
    val o = ctx.freshName("o")

    val compilerTerms =
      RuleEngineRunnerUtils.genCompilerTerms[T](ctx, PassThroughCompileEvals(children), expressionOffsets, children,
        debugMode, variablesPerFunc, variableFuncGroup, false,
        // capture the current
        extraResult = (outArrTerm: String) =>
          s"""
             if (($outArrTerm != null) || $includeNulls) {
                if (($outArrTerm == null) || ${!(flatten && canFlatten)}) {
                  $bufferTerm.addOne($outArrTerm);
                } else {
                  // flatten case and non-null
                  ArrayData $arrayData = (ArrayData) $outArrTerm;
                  for (int $z = 0; $z < $arrayData.numElements(); $z++) {
                    Object $o = ${CodeGenerator.getValue(arrayData, elementType, z)};
                    if (($o != null) || $includeNulls) {
                      $bufferTerm.addOne( $o );
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
      if (debugMode)
        ev.copy(code = code"""
          $pre

          InternalRow ${ev.value} =
            com.sparkutils.quality.impl.CollectRunnerUtils.compiledEvalDebug(
              $utilsName.evalArray($ruleSuitTerm, $ruleSuiteArrays, $resArrTerm),
            ($currentOutputIndex < 0) ? null : com.sparkutils.quality.impl.RuleEngineRunnerUtils.debugOutput($salienceArrTerm, $outArrTerm, $currentOutputIndex));

          $post
          """
        )
      else
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
case class CollectRunnerRunner(ruleSuite: RuleSuite, children: Seq[Expression], resultDataType: DataType,
                                debugMode: Boolean, variablesPerFunc: Int,
                                variableFuncGroup: Int, expressionOffsets: Array[Int],
                               flatten: Boolean, includeNulls: Boolean
                               ) extends CollectRunnerBase[CollectRunnerRunner] {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = doGenCodeI(ctx, ev)

  override implicit val classTagT: ClassTag[CollectRunnerRunner] = ClassTag(classOf[CollectRunnerRunner])
  override val tClass: Class[CollectRunnerRunner] = classOf[CollectRunnerRunner]
}
