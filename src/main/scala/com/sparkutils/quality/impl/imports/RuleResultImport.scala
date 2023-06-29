package com.sparkutils.quality.impl.imports

import com.sparkutils.quality.impl.RuleResultExpression
import org.apache.spark.sql.Column

trait RuleResultImport {

  /**
   * Retrieves the rule result for a given id, the result type is dependent on ruleSuiteResults's type.  Integer is returned for DQ checks and either String or (ruleResult: String, resultDDL: String) for ExpressionResults.
   *
   * @param ruleSuiteResults
   * @param ruleSuiteId
   * @param ruleSetId
   * @param ruleId
   * @return
   */
  def rule_result(ruleSuiteResults: Column, ruleSuiteId: Column, ruleSetId: Column, ruleId: Column): Column =
    RuleResultExpression(ruleSuiteResults, ruleSuiteId, ruleSetId, ruleId)
}
