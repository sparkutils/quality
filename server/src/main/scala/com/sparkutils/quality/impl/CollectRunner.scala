package com.sparkutils.quality.impl

import com.sparkutils.quality._
import com.sparkutils.quality.impl.RuleEngineRunnerUtils.flattenExpressions
import com.sparkutils.quality.impl.imports.RuleFolderRunnerImports
import com.sparkutils.quality.impl.util.{PassThroughCompileEvals, PassThroughEvalOnly}
import org.apache.spark.sql.Column
import org.apache.spark.sql.ShimUtils.column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{CreateArray, Expression, NonSQLExpression}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


private[quality] object CollectRunnerUtils extends RuleFolderRunnerImports {

  def compiledEval[T](results: InternalRow, output: ArrayBuffer[T]): InternalRow =
    InternalRow(results, new GenericArrayData(output))

  def addOne[T](output: ArrayBuffer[T], an: T): Unit = output.+=(an)

}

object CollectRunner {

  /**
   * Creates a column that runs the folding RuleSuite.  This also forces registering the lambda functions used by that RuleSuite.
   *
   * FolderRunner runs all output expressions for matching rules in order of salience, the startingStruct is passed ot the first
   * matching, the result passed to the second etc.  In contrast to ruleEngineRunner OutputExpressions should be lambdas with one parameter, that of the structure
   *
   * @param ruleSuite The ruleSuite with runOnPassProcessors
   * @param resultDataType the collected result type, specify this if the derived types have nullability or ordering issues.  By default, it takes the type of the last output expression
   * @param variablesPerFunc Defaulting to 40 allows, in combination with variableFuncGroup allows customisation of handling the 64k jvm method size limitation when performing WholeStageCodeGen
   * @param variableFuncGroup Defaulting to 20
   * @param flatten when resultType is an ArrayType should the result be flattened
   * @param includeNulls should nulls returned by the output expressions be included, note when flattening nulls IN the returned arrays are not filtered
   * @return A Column representing the QualityRules expression built from this ruleSuite
   */
  def collectRunnerClassic(ruleSuite: RuleSuite, resultDataType: Option[DataType] = None, variablesPerFunc: Int = 40,
                           variableFuncGroup: Int = 20,
                           flatten: Boolean = true, includeNulls: Boolean = false): Column = {
    com.sparkutils.quality.registerLambdaFunctions( ruleSuite.lambdaFunctions )

    val (expressions, indexes, triggerCount) = flattenExpressions(ruleSuite)

    val cleaned = RuleLogicUtils.cleanExprs(ruleSuite)

    column(
      CollectRunnerRunner(cleaned, expressions, resultDataType,
        variablesPerFunc, variableFuncGroup,
        expressionOffsets = indexes, triggerCount = triggerCount, flatten = flatten, includeNulls = includeNulls)
    )
  }
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

  lazy val actualType = {
    val theType = resultDataType.getOrElse{ children.last.dataType }
    /*
    // if a type is supplied are all output expressions compatible with it
    children.drop(triggerCount).find(e => e.dataType != theType).foreach{ e =>
      throw new QualityException(s"CollectRunner DataType ${e.dataType.sql} does not match the last OutputExpression type ${theType.sql}")
    }*/
    theType
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
      StructField(name = "ruleSuiteResults", dataType = impl.types.ruleSuiteResultType),
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
      RuleEngineRunnerUtils.genCompilerTerms[T](ctx, PassThroughEvalOnly(children), expressionOffsets, children,
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
