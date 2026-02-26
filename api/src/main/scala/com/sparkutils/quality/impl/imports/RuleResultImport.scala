package com.sparkutils.quality.impl.imports

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
   * Groups runnerResults which represent the result column from a ruleRunner, expressionRunner excluded.  The allowed input types are:
   *
   * - array(ruleRunner DQ results) which returns RuleSuiteGroupResults
   * - array(ruleEngineResults) which returns either (RuleSuiteGroupResults, array((salientRule, result))) or
   *    (RuleSuiteGroupResults, array((salientRule, array((salience,result)))) for debug
   * - array(ruleFolderResults) which returns either (RuleSuiteGroupResults, array(result)) or
   *    (RuleSuiteGroupResults, array(array((salience,result))) for debug
   * - array(collectRunner results) which returns (RuleSuiteGroupResults, array(result)), you can also use processResult to flatten
   * @param runnerResults the runner result column
   * @param processResult a lambda to process the result column, not applicable to DQ results, this removes the need for an additional select/projection to process results
   * @return
   */
  def group_results(runnerResults: Column, processResult: Option[Column => Column] = None): Column =
    processResult.map(f => ShimUtils.callFunction("group_results", runnerResults, LambdaFunctions.createLambda(f))).
      getOrElse( ShimUtils.callFunction("group_results", runnerResults) )
}
