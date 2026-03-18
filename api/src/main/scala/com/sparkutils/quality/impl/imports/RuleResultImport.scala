package com.sparkutils.quality.impl.imports

import com.sparkutils.quality.functions.pack_ints
import com.sparkutils.shim.LambdaFunctions
import org.apache.spark.sql.ShimUtils.callFunction
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, ShimUtils}

trait RuleResultImport {

  /**
   * Retrieves the rule result for a given id, the result type is dependent on ruleSuiteResults's type.
   * Integer is returned for DQ checks and either String or (ruleResult: String, resultDDL: String) for ExpressionResults.
   *
   * @param ruleSuiteResults
   * @param ruleSuiteId
   * @param ruleSetId
   * @param ruleId
   * @return
   */
  def rule_result(ruleSuiteResults: Column, ruleSuiteId: Column, ruleSetId: Column, ruleId: Column): Column =
    ShimUtils.callFunction("rule_result", ruleSuiteResults, ruleSuiteId, ruleSetId, ruleId)

  /**
   * Retrieves the rule result for a given id, the result type is dependent on ruleSuiteResults's type.
   * Integer is returned for DQ checks and either String or (ruleResult: String, resultDDL: String) for ExpressionResults.
   *
   * @param ruleSuiteResults
   * @param ruleSuiteId
   * @param ruleSetId
   * @param ruleId
   * @return
   */
  def rule_result(ruleSuiteResults: Column, ruleSuiteId: Column, ruleSuiteVersion: Column,
                  ruleSetId: Column, ruleSetVersion: Column,
                  ruleId: Column, ruleVersion: Column): Column =
    ShimUtils.callFunction("rule_result", ruleSuiteResults,
      pack_ints(ruleSuiteId, ruleSuiteVersion),
      pack_ints(ruleSetId, ruleSetVersion),
      pack_ints(ruleId, ruleVersion))

  /**
   * Retrieves the rule result for a given id, the result type is dependent on ruleSuiteResults's type.
   * Integer is returned for DQ checks and either String or (ruleResult: String, resultDDL: String) for ExpressionResults.
   *
   * @param ruleSuiteResults
   * @param ruleSuiteId
   * @param ruleSetId
   * @param ruleId
   * @return
   */
  def rule_result(ruleSuiteResults: Column, ruleSuiteId: Int, ruleSuiteVersion: Int,
                  ruleSetId: Int, ruleSetVersion: Int,
                  ruleId: Int, ruleVersion: Int): Column =
    ShimUtils.callFunction("rule_result", ruleSuiteResults,
      pack_ints(ruleSuiteId, ruleSuiteVersion),
      pack_ints(ruleSetId, ruleSetVersion),
      pack_ints(ruleId, ruleVersion))

  /**
   * Groups runnerResults which represent the result column from an array of rule runners, expressionRunner excluded, or
   * an array of RuleSuiteGroupResults.  Note many of the results will contain nullable fields,
   * see GroupResultsTest for example result types with Scala Encoders.  The allowed input types are:
   *
   * - array(ruleRunner DQ results) which returns RuleSuiteGroupResults
   * - array(ruleEngineResults) which returns either (RuleSuiteGroupResults, array((salientRule, result))) or
   *    (RuleSuiteGroupResults, array((salientRule, array((salience,result)))) for debug
   * - array(ruleFolderResults) which returns either (RuleSuiteGroupResults, array(result)) or
   *    (RuleSuiteGroupResults, array(array((salience,result))) for debug
   * - array(collectRunner results) which returns (RuleSuiteGroupResults, array(result)), you can also use processResult to flatten
   * - array( group_results( array( group_results( collect_runner ... will return (RuleSuiteGroupResults, array(array(array(X))))
   * @param runnerResults the runner result column
   * @param processResult a lambda to process the result column, not applicable to DQ results, this removes the need for an
   *                      additional select/projection to process results, ideal for calling flatten on results
   * @return
   */
  def group_results(runnerResults: Column, processResult: Option[Column => Column] = None): Column =
    processResult.map(f => ShimUtils.callFunction("group_results", runnerResults, LambdaFunctions.createLambda(f))).
      getOrElse( ShimUtils.callFunction("group_results", runnerResults) )

  /**
   * Converts non-debug engine results into a single result column, allowing engine, collector and folder results to be nested
   * and combined via group_results within the same nested rule suite calls.  Similarly, non-debug RuleSuiteGroupResults can be
   * converted.
   * @return
   */
  def unify_result(runnerResults: Column): Column =
    ShimUtils.callFunction("unify_result", runnerResults)

}
