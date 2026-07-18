package com.sparkutils.quality.impl.imports

import com.sparkutils.quality.RuleSuite
import com.sparkutils.quality.impl.{RuleEngineRunnerImpl, RuleSuiteHelpers}
import org.apache.spark.sql.functions.{lit, typedLit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, ShimUtils}

trait ClassicRuleEngineRunnerImports {

  /**
   * Creates a column that runs the RuleSuite.  This also forces registering the lambda functions used by that RuleSuite.
   *
   * NOTE if using this in a Connect session resolveWith, compileEvals, forceRunnerEval and forceTriggerEval are ignored and use defaults
   *
   * @param ruleSuite The ruleSuite with runOnPassProcessors
   * @param resultDataType The type of the results from runOnPassProcessors - must be the same for all result types,
   *                       by default most fields will be nullable and encoding must follow the fields when not specified.   *
   * @param compileEvals Should the rules be compiled out to interim objects - by default false
   * @param debugMode When debugMode is enabled the resultDataType is wrapped in Array of (salience, result)
   *                  pairs to ease debugging
   * @param resolveWith This experimental parameter can take the DataFrame these rules will be added to and pre-resolve
   *                    and optimise the sql expressions, see the documentation for details on when to and not to use this.
   * @param variablesPerFunc Defaulting to 40 allows, in combination with variableFuncGroup allows customisation of
   *                         handling the 64k jvm method size limitation when performing WholeStageCodeGen
   * @param variableFuncGroup Defaulting to 20
   * @param forceRunnerEval Defaulting to false, passing true forces a simplified partially interpreted evaluation
   *                        (compileEvals must be false to get fully interpreted)
   * @param forceTriggerEval Defaulting to false
   * @return A Column representing the QualityRules expression built from this ruleSuite
   */
  def ruleEngineRunner(ruleSuite: RuleSuite, resultDataType: Option[DataType] = None, compileEvals: Boolean = false,
                       debugMode: Boolean = false, resolveWith: Option[DataFrame] = None, variablesPerFunc: Int = 40,
                       variableFuncGroup: Int = 20, forceRunnerEval: Boolean = false, forceTriggerEval: Boolean = false,
                       extraConfig: Map[String, String] = Map.empty): Column =
    if (ResolveUtil.checkResolveMakesSenseOrClassic(resolveWith))
      RuleEngineRunnerImpl.ruleEngineRunnerImpl(ruleSuite, resultDataType, compileEvals, debugMode, resolveWith, variablesPerFunc,
        variableFuncGroup, forceRunnerEval, forceTriggerEval, extraConfig)
    else
      ShimUtils.callFunction("rule_engine_runner", lit(RuleSuiteHelpers.serialize(ruleSuite)),
        lit(resultDataType.map(_.sql).getOrElse("")), lit(debugMode), lit(variablesPerFunc),
        lit(variableFuncGroup), typedLit(extraConfig)
      )

  /**
   * Creates a column that runs the RuleSuite.  This also forces registering the lambda functions used by that RuleSuite
   * @param ruleSuite The ruleSuite with runOnPassProcessors
   * @param resultDataType The type of the results from runOnPassProcessors - must be the same for all result types,
   *                       by default most fields will be nullable and encoding must follow the fields when not specified.   *
   * @param debugMode When debugMode is enabled the resultDataType is wrapped in Array of (salience, result)
   *                  pairs to ease debugging
   * @return A Column representing the QualityRules expression built from this ruleSuite
   */
  def ruleEngineRunner(ruleSuite: RuleSuite, resultDataType: DataType, debugMode: Boolean): Column =
    ruleEngineRunner(ruleSuite, Some(resultDataType), debugMode = debugMode)

  /**
   * Creates a column that runs the RuleSuite.  This also forces registering the lambda functions used by that RuleSuite
   * @param ruleSuite The ruleSuite with runOnPassProcessors
   * @param resultDataType The type of the results from runOnPassProcessors - must be the same for all result types,
   *                       by default most fields will be nullable and encoding must follow the fields when not specified.   *
   * @return A Column representing the QualityRules expression built from this ruleSuite
   */
  def ruleEngineRunner(ruleSuite: RuleSuite, resultDataType: DataType): Column =
    ruleEngineRunner(ruleSuite, Some(resultDataType))
}
