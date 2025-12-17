package com.sparkutils.quality.impl.imports

import com.sparkutils.quality.RuleSuite
import com.sparkutils.quality.impl.{RuleSuiteHelpers, Runners}
import org.apache.spark.sql.ShimUtils.callFunction
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, DataFrame, ShimUtils}

trait RuleRunnerImports {

  /**
   * Creates a column that runs the RuleSuite suitable for DQ / Validation.  This also forces registering the lambda functions used by that RuleSuite  This forwards to the original ruleRunner via dqRuleRunner
   *
   * @param ruleSuite The Qualty RuleSuite to evaluate
   * @param compileEvals Should the rules be compiled out to interim objects - by default true for eval usage, wholeStageCodeGen will evaluate in place
   * @param resolveWith This experimental parameter can take the DataFrame these rules will be added to and pre-resolve and optimise the sql expressions, see the documentation for details on when to and not to use this. RuleRunner does not currently do wholestagecodegen when resolveWith is used.
   * @param variablesPerFunc Defaulting to 40, it allows, in combination with variableFuncGroup customisation of handling the 64k jvm method size limitation when performing WholeStageCodeGen.  You _shouldn't_ need it but it's there just in case.
   * @param variableFuncGroup Defaulting to 20
   * @param forceRunnerEval Defaulting to false, passing true forces a simplified partially interpreted evaluation (compileEvals must be false to get fully interpreted)
   * @return A Column representing the Quality DQ expression built from this ruleSuite
   */
  def ruleRunner(ruleSuite: RuleSuite, compileEvals: Boolean = true, resolveWith: Option[DataFrame] = None, variablesPerFunc: Int = 40, variableFuncGroup: Int = 20, forceRunnerEval: Boolean = false): Column =
    dqRuleRunner(ruleSuite, compileEvals, resolveWith, variablesPerFunc, variableFuncGroup, forceRunnerEval)

  /**
   * Creates a column that runs the RuleSuite suitable for DQ / Validation.  This also forces registering the lambda functions used by that RuleSuite.
   *
   * @param ruleSuite The Qualty RuleSuite to evaluate
   * @param compileEvals Should the rules be compiled out to interim objects - by default true for eval usage, wholeStageCodeGen will evaluate in place
   * @param resolveWith This experimental parameter can take the DataFrame these rules will be added to and pre-resolve and optimise the sql expressions, see the documentation for details on when to and not to use this. RuleRunner does not currently do wholestagecodegen when resolveWith is used.
   * @param variablesPerFunc Defaulting to 40, it allows, in combination with variableFuncGroup customisation of handling the 64k jvm method size limitation when performing WholeStageCodeGen.  You _shouldn't_ need it but it's there just in case.
   * @param variableFuncGroup Defaulting to 20
   * @param forceRunnerEval Defaulting to false, passing true forces a simplified partially interpreted evaluation (compileEvals must be false to get fully interpreted)
   * @return A Column representing the Quality DQ expression built from this ruleSuite
   */
  def dqRuleRunner(ruleSuite: RuleSuite, compileEvals: Boolean = true, resolveWith: Option[DataFrame] = None, variablesPerFunc: Int = 40, variableFuncGroup: Int = 20, forceRunnerEval: Boolean = false): Column =
    Runners.ruleRunner(ruleSuite, compileEvals, resolveWith, variablesPerFunc, variableFuncGroup, forceRunnerEval).getOrElse(
      ShimUtils.callFunction("dq_rule_runner", lit(RuleSuiteHelpers.serialize(ruleSuite)), lit(compileEvals), lit(variablesPerFunc), lit(variableFuncGroup), lit(forceRunnerEval))
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
   * The integer value for passed dq rules
   */
  val PassedInt = RuleResultsImports.PassedInt
  /**
   * The integer value for failed dq rules
   */
  val FailedInt = RuleResultsImports.FailedInt

}

object RuleResultsImports {

  val SoftFailedInt = -1
  val DisabledRuleInt = -2
  val PassedInt = 100000
  val FailedInt = 0

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