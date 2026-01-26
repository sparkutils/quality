package com.sparkutils.quality.impl.imports

import com.sparkutils.quality.RuleSuite
import com.sparkutils.quality.impl.RuleSuiteHelpers
import org.apache.spark.sql.{Column, ShimUtils}
import org.apache.spark.sql.functions.{lit, typedLit}

trait ExpressionRunnerImports {

  /**
   * Runs the ruleSuite expressions saving results as a tuple of (ruleResult: String, resultDDL: String)
   * @param ruleSuite
   * @param name
   * @return
   */
  def typedExpressionRunner(ruleSuite: RuleSuite, ddlType: String, name: String = "expressionResults"): Column =
    ShimUtils.callFunction("typed_expression_runner", lit(RuleSuiteHelpers.serialize(ruleSuite)), lit(ddlType), lit(name))

  def expressionRunner(ruleSuite: RuleSuite, name: String = "expressionResults", renderOptions: Map[String, String] = Map.empty): Column =
    ShimUtils.callFunction("expression_runner", lit(RuleSuiteHelpers.serialize(ruleSuite)), lit(name), typedLit(renderOptions))

}

trait StripResultTypesFunction {

  /**
   * Stores only the ruleResult, removing the structure including the resultDDL column
   *
   * @param expressionResults
   * @return
   */
  def strip_result_ddl(expressionResults: Column): Column =
    ShimUtils.callFunction("strip_result_ddl", expressionResults)
}
