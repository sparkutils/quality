package com.sparkutils.quality.impl.imports

import com.sparkutils.quality.RuleSuite
import com.sparkutils.quality.impl.RuleEngineRunnerUtils.flattenExpressions
import com.sparkutils.quality.impl.util.{NonPassThrough, PassThroughCompileEvals, PassThroughEvalOnly}
import com.sparkutils.quality.impl.{CollectRunnerRunner, RuleFolderRunner, RuleFolderRunnerEval, RuleLogicUtils}
import org.apache.spark.sql.ShimUtils.{column, expression}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.qualityFunctions.{FunN, RefExpressionLazyType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, QualitySparkUtils}

import java.util.concurrent.atomic.AtomicReference

trait CollectRunnerImports {

  /**
   * Creates a column that runs the folding RuleSuite.  This also forces registering the lambda functions used by that RuleSuite.
   *
   * FolderRunner runs all output expressions for matching rules in order of salience, the startingStruct is passed ot the first
   * matching, the result passed to the second etc.  In contrast to ruleEngineRunner OutputExpressions should be lambdas with one parameter, that of the structure
   *
   * @param ruleSuite The ruleSuite with runOnPassProcessors
   * @param resultType the collected result type
   * @param compileEvals Should the rules be compiled out to interim objects - by default false, allowing optimisations
   * @param debugMode When debugMode is enabled the resultDataType is wrapped in Array of (salience, result) pairs to ease debugging
   * @param variablesPerFunc Defaulting to 40 allows, in combination with variableFuncGroup allows customisation of handling the 64k jvm method size limitation when performing WholeStageCodeGen
   * @param variableFuncGroup Defaulting to 20
   * @param flatten when resultType is an ArrayType should the result be flattened
   * @param includeNulls should nulls returned by the output expressions be included, note when flattening nulls IN the returned arrays are not filtered
   * @return A Column representing the QualityRules expression built from this ruleSuite
   */
  def collectRunner(ruleSuite: RuleSuite, resultType: DataType,
                       debugMode: Boolean = false, variablesPerFunc: Int = 40,
                       variableFuncGroup: Int = 20,
                      flatten: Boolean = true, includeNulls: Boolean = false): Column = {
    com.sparkutils.quality.registerLambdaFunctions( ruleSuite.lambdaFunctions )

    val (expressions, indexes) = flattenExpressions(ruleSuite)

    val cleaned = RuleLogicUtils.cleanExprs(ruleSuite)

    column(
      CollectRunnerRunner(cleaned, expressions, resultType,
        debugMode = false, variablesPerFunc, variableFuncGroup, // TODO
        expressionOffsets = indexes, flatten = flatten, includeNulls = includeNulls)
    )
  }
}
