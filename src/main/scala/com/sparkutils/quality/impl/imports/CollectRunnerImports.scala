package com.sparkutils.quality.impl.imports

import com.sparkutils.quality.RuleSuite
import com.sparkutils.quality.impl.CollectRunner.{UnrollOutputArray, UseInPlaceArray}
import com.sparkutils.quality.impl.RuleEngineRunnerUtils.flattenExpressions
import com.sparkutils.quality.impl.{CollectRunnerRunner, InPlaceArray, RuleLogicUtils}
import com.sparkutils.shim.expressions.Names
import org.apache.spark.sql.Column
import org.apache.spark.sql.ShimUtils.column
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.apache.spark.sql.types._


trait CollectRunnerImports {

  /**
   * Creates a column that runs the folding RuleSuite.  This also forces registering the lambda functions used by that RuleSuite.
   *
   * FolderRunner runs all output expressions for matching rules in order of salience, the startingStruct is passed ot the first
   * matching, the result passed to the second etc.  In contrast to ruleEngineRunner OutputExpressions should be lambdas with one parameter, that of the structure
   *
   * By default, InPlaceArray will substitute CreateArray (array sql function), should there be issues with loop unrolling com.sparkutils.collect.useInPlaceArray can be set to false.
   *
   * @param ruleSuite The ruleSuite with runOnPassProcessors
   * @param resultDataType the collected result type, specify this if the derived types have nullability or ordering issues.  By default, it takes the type of the last output expression
   * @param variablesPerFunc Defaulting to 40 allows, in combination with variableFuncGroup allows customisation of handling the 64k jvm method size limitation when performing WholeStageCodeGen
   * @param variableFuncGroup Defaulting to 20
   * @param flatten when resultType is an ArrayType should the result be flattened
   * @param includeNulls should nulls returned by the output expressions be included, note when flattening nulls IN the returned arrays are not filtered
   * @return A Column representing the QualityRules expression built from this ruleSuite
   */
  def collectRunner(ruleSuite: RuleSuite, resultDataType: Option[DataType] = None, variablesPerFunc: Int = 40,
                           variableFuncGroup: Int = 20, flatten: Boolean = true, includeNulls: Boolean = false,
                           useInPlaceArray: Boolean = true, unrollInPlaceArray: Boolean = false,
                           unrollOutputArraySize: Int = 1): Column = {
    import com.sparkutils.quality.getConfig
    com.sparkutils.quality.registerLambdaFunctions( ruleSuite.lambdaFunctions )

    val (expressionsRaw, indexes, triggerCount) = flattenExpressions(ruleSuite)

    val cleaned = RuleLogicUtils.cleanExprs(ruleSuite)

    val inPlace = getConfig(UseInPlaceArray, s"$useInPlaceArray").toBoolean
    val unroll = getConfig(UnrollOutputArray, s"$unrollInPlaceArray").toBoolean

    val canUnroll =
      expressionsRaw.drop(triggerCount).map{
        case a: UnresolvedFunction if Names.toName(a).toLowerCase == "array" => a.children.size
        case _ => -1
      }.toArray

    val expressions =
      if (flatten && inPlace)
        expressionsRaw.zipWithIndex.map{
          case (a: UnresolvedFunction, i) if // check triggerCount because we do not want triggers to be swapped
            Names.toName(a).toLowerCase == "array" && i >= triggerCount => InPlaceArray(a.children) // TODO should this move into the expression and auto resolve in the case of FunNRewrite?
          case (e, i) => e
        }
      else
        expressionsRaw

    val isInPlace =
      expressions.drop(triggerCount).map{
        case _: InPlaceArray => true
        case _ => false
      }.toArray

    column(
      CollectRunnerRunner(cleaned, expressions, resultDataType,
        variablesPerFunc, variableFuncGroup,
        expressionOffsets = indexes, triggerCount = triggerCount, flatten = flatten,
        includeNulls = includeNulls, canUnroll = canUnroll, isInPlace = isInPlace, unroll = unroll,
        unrollOutputArraySize = unrollOutputArraySize)
    )
  }

}
