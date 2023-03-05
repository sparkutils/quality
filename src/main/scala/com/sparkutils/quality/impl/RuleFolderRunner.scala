package com.sparkutils.quality.impl

import com.sparkutils.quality.impl.RuleEngineRunnerUtils.flattenExpressions
import com.sparkutils.quality.utils.{NonPassThrough, PassThrough}
import com.sparkutils.quality._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, NonSQLExpression}
import org.apache.spark.sql.qualityFunctions.{FunN, RefExpressionLazyType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, QualitySparkUtils}

import java.util.concurrent.atomic.AtomicReference

trait RuleFolderRunnerImports {

  /**
   * Creates a column that runs the folding RuleSuite.  This also forces registering the lambda functions used by that RuleSuite.
   *
   * FolderRunner runs all output expressions for matching rules in order of salience, the startingStruct is passed ot the first
   * matching, the result passed to the second etc.  In contrast to ruleEngineRunner OutputExpressions should be lambdas with one parameter, that of the structure
   *
   * @param ruleSuite The ruleSuite with runOnPassProcessors
   * @param startingStruct This struct is passed to the first matching rule, ideally you would use the spark dsl struct function to refer to existing columns
   * @param compileEvals Should the rules be compiled out to interim objects - by default true
   * @param debugMode When debugMode is enabled the resultDataType is wrapped in Array of (salience, result) pairs to ease debugging
   * @param resolveWith This experimental parameter can take the DataFrame these rules will be added to and pre-resolve and optimise the sql expressions, see the documentation for details on when to and not to use this.
   * @param variablesPerFunc Defaulting to 40 allows, in combination with variableFuncGroup allows customisation of handling the 64k jvm method size limitation when performing WholeStageCodeGen
   * @param variableFuncGroup Defaulting to 20
   * @param forceRunnerEval Defaulting to false, passing true forces a simplified partially interpreted evaluation (compileEvals must be false to get fully interpreted)
   * @param forceTriggerEval Defaulting to true, passing true forces each trigger expression to be compiled (compileEvals) and used in place, false instead expands the trigger in-line giving possible performance boosts based on JIT.  Most testing has however shown this not to be the case hence the default, ymmv.
   * @param useType In the case you must use select and can't use withColumn you may provide a type directly to stop the NPE
   * @return A Column representing the QualityRules expression built from this ruleSuite
   */
  def ruleFolderRunner(ruleSuite: RuleSuite, startingStruct: Column, compileEvals: Boolean = true,
                       debugMode: Boolean = false, resolveWith: Option[DataFrame] = None, variablesPerFunc: Int = 40,
                       variableFuncGroup: Int = 20, forceRunnerEval: Boolean = false, useType: Option[StructType] = None,
                       forceTriggerEval: Boolean = true): Column = {
    com.sparkutils.quality.registerLambdaFunctions( ruleSuite.lambdaFunctions )

    // needed to resolve variables
    val dataRef = new AtomicReference[DataType]()

    val realType = () => {
      val starter =
        useType.getOrElse(dataRef.get())
      if (debugMode)
      // wrap it in an array with the priority result
        ArrayType(StructType(Seq(StructField("salience", IntegerType), StructField("result", starter))))
      else
        starter
    }

    val liftLambda = (e: Expression) => FunN(Seq(RefExpressionLazyType(() => dataRef.get(), true)), e)

    val (expressions, indexes) = flattenExpressions(ruleSuite, liftLambda)

    val runner = new RuleFolderRunner(ruleSuite, startingStruct.expr, PassThrough( expressions ), realType, compileEvals = compileEvals,
      debugMode = debugMode, variablesPerFunc, variableFuncGroup, forceRunnerEval = forceRunnerEval, expressionOffsets = indexes, dataRef, forceTriggerEval)

    new Column(
      QualitySparkUtils.resolveWithOverride(resolveWith).map { df =>
        val resolved = QualitySparkUtils.resolveExpression(df, runner)

        resolved.asInstanceOf[RuleFolderRunner].copy(right = resolved.children(1) match {
          // replace the expr
          case PassThrough(children) => NonPassThrough(children)
        })
      } getOrElse runner
    )
  }
}

private[quality] object RuleFolderRunnerUtils extends RuleFolderRunnerImports {

  /**
   * Needs sorting in salience order for output processing but NOT for result's.
   * So output array should use offsets, results should be offset, but order
   * of calling must be salience based.
   *
   */

  def compiledEvalDebug[T](results: InternalRow, output: T): InternalRow =
    InternalRow(results, output)

  def compiledEval[T](results: InternalRow, currentSalience: Int, rules: Array[(Long, Long, Long)], currentOutputIndex: Int, output: Array[T]): InternalRow =
    InternalRow(results,
      if (currentSalience == java.lang.Integer.MAX_VALUE)
        null
      else output(currentOutputIndex)
    )

}

/**
  * Children will be rewritten by the plan, it's then re-incorporated into ruleSuite
  * expressionOffsets.length is the length of the trigger expressions in realChildren, realChildren(expressionOffsets.length + expressionOffsets(x)) will be the correct OutputExpression
  */
case class RuleFolderRunner(ruleSuite: RuleSuite, left: Expression, right: Expression, resultDataType: () => DataType,
                            compileEvals: Boolean, debugMode: Boolean, variablesPerFunc: Int,
                            variableFuncGroup: Int, forceRunnerEval: Boolean, expressionOffsets: Array[Int],
                            dataRef: AtomicReference[DataType], forceTriggerEval: Boolean) extends BinaryExpression with NonSQLExpression with CodegenFallback {

  // hack to push type through to lambda's, resolution only happens on driver but only works with a projection e.g. withColumn or introducing an extra column
  if (left.resolved) {
    dataRef.set(left.dataType)
  }

  import RuleEngineRunnerUtils._
  import RuleFolderRunnerUtils._

  val startingStruct: Expression = left

  lazy val realChildren =
    right match {
      case r @ NonPassThrough(_) => r.rules
      case PassThrough(children) => children
    }

  // only used for compilation
  lazy val compiledRealChildren = realChildren.slice(0, expressionOffsets.length).map(ExpressionWrapper(_, compileEvals)).toArray

  override def nullable: Boolean = false
  override def toString: String = s"RuleFolderRunner(${realChildren.mkString(", ")})"

  // used only for eval, compiled uses the children directly
  lazy val reincorporated = reincorporateExpressions(ruleSuite, realChildren, compileEvals, expressionOffsets)

  // keep it simple for this one. - can return an internal row or whatever..
  override def eval(input: InternalRow): Any = {
    val starter = startingStruct.eval(input).asInstanceOf[InternalRow] // TODO - throw a decent error message at ruleFolder call
    val (res, processedRes) = //(null, null)
      reincorporated.foldWithProcessors(input, starter, debugMode)
    InternalRow(com.sparkutils.quality.impl.RuleRunnerUtils.ruleResultToRow(res), processedRes)
  }

  def dataType: DataType = StructType( Seq(
      StructField(name = "ruleSuiteResults", dataType = com.sparkutils.quality.ruleSuiteResultType),
      StructField(name = "result", dataType = resultDataType(), nullable = true)
    ))

  override protected def doGenCode(ctx:  _root_.org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext, ev:  _root_.org.apache.spark.sql.catalyst.expressions.codegen.ExprCode): _root_.org.apache.spark.sql.catalyst.expressions.codegen.ExprCode = {
    if (forceRunnerEval) {
      return super[CodegenFallback].doGenCode(ctx, ev)
    }

    ctx.references += this

    // need to setup the folder variable to pass around, create it with "left"
    // thread it through
    val folderV = ctx.addMutableState( "InternalRow",
      ctx.freshName("folderV") )

    val ruleRunnerExpressionIdx = ctx.references.size - 1
    val ruleRunnerClassName = classOf[RuleFolderRunner].getName
    val funName = classOf[FunN].getName
    val lazyRefName = classOf[RefExpressionLazyType].getName

    // order by salience
    val salience = com.sparkutils.quality.impl.RuleEngineRunnerUtils.flattenSalience(ruleSuite)
    val outputs = 0 until (realChildren.size - expressionOffsets.size)
    val reordered = outputs zip salience sortBy(_._2) map(_._1)

    val offsetTerm = ctx.addMutableState("int", ctx.freshName("offset"),
      v => s"$v = ${expressionOffsets.size};")

    val compilerTerms =
      RuleEngineRunnerUtils.genCompilerTerms[RuleFolderRunner](ctx, right, expressionOffsets, realChildren,
        debugMode, variablesPerFunc, variableFuncGroup, forceTriggerEval,
        // capture the current
        extraResult = (outArrTerm: String) => s"$folderV = $outArrTerm;",
        extraSetup = (idx: String) =>
          // set the current
          s"(($lazyRefName)(($funName)(($ruleRunnerClassName)references[$ruleRunnerExpressionIdx]).realChildren().apply($offsetTerm + $idx)).arguments().apply(0)).value_$$eq($folderV);",
        orderOffset = (idx: Int) => reordered(idx),
        // we shouldn't check salience as we are already ordered by it
        salienceCheck = false
      ).getOrElse(return super[CodegenFallback].doGenCode(ctx, ev))

    import compilerTerms._

    // generate the starting struct
    val starterEval = left.genCode(ctx)

    val pre = s"""
          $currentSalience = java.lang.Integer.MAX_VALUE;
          $currentOutputIndex = -1;

          // starting
          ${starterEval.code}
          // setting the folder
          $folderV = ${starterEval.isNull} ? null : (InternalRow)${starterEval.value}; \n

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
            com.sparkutils.quality.impl.RuleFolderRunnerUtils.compiledEvalDebug(
              $utilsName.evalArray($ruleSuitTerm, $ruleSuiteArrays, $resArrTerm),
            ($currentOutputIndex < 0) ? null : com.sparkutils.quality.impl.RuleEngineRunnerUtils.debugOutput($salienceArrTerm, $outArrTerm, $currentOutputIndex));

          $post
          """
        )
      else
        ev.copy(code = code"""
          $pre

          InternalRow ${ev.value} =
            com.sparkutils.quality.impl.RuleFolderRunnerUtils.compiledEval(
              $utilsName.evalArray($ruleSuitTerm, $ruleSuiteArrays, $resArrTerm),
              $currentSalience, $ruleTupleArrTerm, $currentOutputIndex, $outArrTerm);

          $post
          """
        )

    res

  }

  protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}
