package com.sparkutils.quality.impl.imports

import com.sparkutils.quality.RuleSuite
import com.sparkutils.quality.impl.{CallFunctionImpls, RuleSuiteHelpers, Runners}
import org.apache.spark.sql.ShimUtils.callFunction
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, DataFrame, ShimUtils}

trait RuleRunnerImports {

  /**
   * Creates a column that runs the RuleSuite suitable for DQ / Validation.  This also forces registering the lambda functions used by that RuleSuite  This forwards to the original ruleRunner via dqRuleRunner
   *
   * @param ruleSuite The Qualty RuleSuite to evaluate
   * @param variablesPerFunc Defaulting to 40, it allows, in combination with variableFuncGroup customisation of handling the 64k jvm method size limitation when performing WholeStageCodeGen.  You _shouldn't_ need it but it's there just in case.
   * @param variableFuncGroup Defaulting to 20
   * @return A Column representing the Quality DQ expression built from this ruleSuite
   */
  def ruleRunner(ruleSuite: RuleSuite, variablesPerFunc: Int = 40, variableFuncGroup: Int = 20): Column =
    dqRuleRunner(ruleSuite, variablesPerFunc, variableFuncGroup)

  /**
   * Creates a column that runs the RuleSuite suitable for DQ / Validation.  This also forces registering the lambda functions used by that RuleSuite.
   *
   * @param ruleSuite The Qualty RuleSuite to evaluate
   * @param variablesPerFunc Defaulting to 40, it allows, in combination with variableFuncGroup customisation of handling the 64k jvm method size limitation when performing WholeStageCodeGen.  You _shouldn't_ need it but it's there just in case.
   * @param variableFuncGroup Defaulting to 20
   * @return A Column representing the Quality DQ expression built from this ruleSuite
   */
  def dqRuleRunner(ruleSuite: RuleSuite, variablesPerFunc: Int = 40, variableFuncGroup: Int = 20): Column =
    Runners.ruleRunner(ruleSuite, variablesPerFunc = variablesPerFunc, variableFuncGroup = variableFuncGroup).getOrElse(
      CallFunctionImpls.dq( lit(RuleSuiteHelpers.serialize(ruleSuite)), variablesPerFunc, variableFuncGroup)
    )

  /**
   * The integer value for soft failed dq rules
   */
  val SoftFailedInt = RuleResultsImports.SoftFailedInt
  /**
   * The integer value for disabled dq rules
   */
  val DisabledRuleInt = RuleResultsImports.DisabledRuleInt
  /**
   * The integer value for ignored dq rules
   */
  val IgnoredRuleInt = RuleResultsImports.IgnoredRuleInt
  /**
   * The integer value for RuleSuiteResult.overallResult when no trigger rules have run and the default rule was
   */
  val DefaultRuleInt = RuleResultsImports.DefaultRuleInt
  /**
   * When Folder is configured with a default rule and debug mode is enabled, this salience is returned
   */
  val DefaultRuleSalience = RuleResultsImports.DisabledRuleSalience
  /**
   * The integer value for passed dq rules
   */
  val PassedInt = RuleResultsImports.PassedInt
  /**
   * The integer value for failed dq rules or engine rules that the trigger has returned false for
   */
  val FailedInt = RuleResultsImports.FailedInt
  /**
   * The integer value for rules, typically ruleEngineRunner, that have not yet been evaluated
   */
  val UnevaluatedRuleInt = RuleResultsImports.UnevaluatedRuleInt
}

object RuleResultsImports {

  val SoftFailedInt = -1
  val DisabledRuleInt = -2
  val DisabledRuleSalience = Integer.MIN_VALUE
  val IgnoredRuleInt = -3
  val DefaultRuleInt = -4
  val PassedInt = 100000
  val FailedInt = 0
  val UnevaluatedRuleInt = -5

}

trait RuleRunnerFunctionImports {
  /**
   * Returns the probability from a given rule result
   * @param result
   * @return
   */
  def probability(result: Column): Column = callFunction("probability", result)

  /**
   * The soft_failed value
   */
  val soft_failed = callFunction("soft_failed")
  /**
   * The disabled_rule value
   */
  val disabled_rule = callFunction("disabled_rule")
  /**
   * The ignored_rule value
   */
  val ignored_rule = callFunction("ignored_rule")
  /**
   * The default_rule value
   */
  val default_rule = callFunction("default_rule")
  /**
   * The passed value
   */
  val passed = callFunction("passed")
  /**
   * The failed value
   */
  val failed = callFunction("failed")

  /**
   * Flattens DQ results, unpacking the nested structure into a simple relation
   * @param result
   * @return
   */
  def flatten_results(result: Column): Column = callFunction("flatten_results", result)

  /**
   * Flattens rule results, unpacking the nested structure into a simple relation
   *
   * @param result
   * @return
   */
  def flatten_rule_results(result: Column): Column = callFunction("flatten_rule_results", result)

  /**
   * Flattens folder results, unpacking the nested structure into a simple relation
   *
   * @param result
   * @return
   */
  def flatten_folder_results(result: Column): Column = callFunction("flatten_folder_results", result)

  /**
   * Consumes a RuleSuiteResult and returns RuleSuiteDetails
   */
  def rule_suite_result_details(result: Column): Column = callFunction("rule_suite_result_details", result)

}