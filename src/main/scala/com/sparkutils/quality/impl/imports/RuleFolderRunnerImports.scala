package com.sparkutils.quality.impl.imports

import com.sparkutils.quality.RuleSuite
import com.sparkutils.quality.impl.RuleEngineRunnerUtils.flattenExpressions
import com.sparkutils.quality.impl.{RuleFolderRunner, RuleFolderRunnerEval, RuleLogicUtils}
import com.sparkutils.quality.impl.util.{NonPassThrough, PassThrough, PassThroughEvalOnly}
import org.apache.spark.sql.ShimUtils.{column, expression}
import org.apache.spark.sql.catalyst.expressions.Expression
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

    val lazyRef = RefExpressionLazyType(dataRef, true)

    val liftLambda = (e: Expression) => FunN(Seq(lazyRef), e)

    val (expressions, indexes) = flattenExpressions(ruleSuite, liftLambda)

    val cleaed = RuleLogicUtils.cleanExprs(ruleSuite)
    val starter = expression(startingStruct)
    val exprs =
      // ExpressionProxy and SubExprEvaluationRuntime cannot be used with compileEvals
      if (compileEvals)
        PassThrough(expressions)
      else
        PassThroughEvalOnly(expressions)

    val runner =
      if (forceRunnerEval || resolveWith.isDefined)
        new RuleFolderRunnerEval(cleaed, starter, exprs,
          realType, compileEvals = compileEvals,
          debugMode = debugMode, variablesPerFunc, variableFuncGroup,
          expressionOffsets = indexes, dataRef, forceTriggerEval)
      else
        new RuleFolderRunner(cleaed, starter, exprs,
          realType, compileEvals = compileEvals,
          debugMode = debugMode, variablesPerFunc, variableFuncGroup,
          expressionOffsets = indexes, dataRef, forceTriggerEval)

    column(
      QualitySparkUtils.resolveWithOverride(resolveWith).map { df =>
        val resolved = QualitySparkUtils.resolveExpression(df, runner)

        resolved.asInstanceOf[RuleFolderRunner].copy(right = resolved.children.head match {
          // replace the expr
          case PassThrough(children) => NonPassThrough(children)
          case PassThroughEvalOnly(children) => NonPassThrough(children)
        })
      } getOrElse runner
    )
  }
}
