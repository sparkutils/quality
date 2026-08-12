package com.sparkutils.quality.impl.imports

import com.sparkutils.quality.RuleSuite
import com.sparkutils.quality.impl.{CallFunctionImpls, RuleSuiteHelpers, Runners}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, ShimUtils}

trait RuleEngineRunnerImports {

  /**
   * Creates a column that runs the RuleSuite.  This also forces registering the lambda functions used by that RuleSuite
   * @param ruleSuite The ruleSuite with runOnPassProcessors
   * @param resultDataType The type of the results from runOnPassProcessors - must be the same for all result types,
   *                       by default most fields will be nullable and encoding must follow the fields when not specified.
   * @param debugMode When debugMode is enabled the resultDataType is wrapped in Array of (salience, result)
   *                  pairs to ease debugging
   * @param variablesPerFunc Defaulting to 40 allows, in combination with variableFuncGroup allows customisation of
   *                         handling the 64k jvm method size limitation when performing WholeStageCodeGen
   * @param variableFuncGroup Defaulting to 20
   * @return A Column representing the QualityRules expression built from this ruleSuite
   */
  def ruleEngineRunner(ruleSuite: RuleSuite, resultDataType: Option[DataType] = None,
                       debugMode: Boolean = false, variablesPerFunc: Int = 40,
                       variableFuncGroup: Int = 20, forceRunnerEval: Boolean = false,
                       forceTriggerEval: Boolean = false, extraConfig: Map[String, String] = Map.empty): Column =
    Runners.ruleEngineRunner(ruleSuite, resultDataType, compileEvals = false, debugMode, None, variablesPerFunc,
      variableFuncGroup, forceRunnerEval, forceTriggerEval, extraConfig = extraConfig).getOrElse(
      CallFunctionImpls.engine( lit(RuleSuiteHelpers.serialize(ruleSuite)), resultDataType,
        debugMode, variablesPerFunc, variableFuncGroup, extraConfig )
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
