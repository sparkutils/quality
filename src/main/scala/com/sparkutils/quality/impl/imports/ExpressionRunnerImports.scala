package com.sparkutils.quality.impl.imports

import com.sparkutils.quality.RuleSuite
import com.sparkutils.quality.impl.{ExpressionRunner, StripResultTypes}
import org.apache.spark.sql.Column
import org.apache.spark.sql.ShimUtils.{column, expression}

trait ExpressionRunnerImports {

  /**
   * Runs the ruleSuite expressions saving results as a tuple of (ruleResult: String, resultDDL: String)
   * @param ruleSuite
   * @param name
   * @return
   */
  def typedExpressionRunner(ruleSuite: RuleSuite, ddlType: String, name: String = "expressionResults") =
    ExpressionRunner(ruleSuite, name, Map.empty, ddlType)

  def expressionRunner(ruleSuite: RuleSuite, name: String = "expressionResults", renderOptions: Map[String, String] = Map.empty) =
    ExpressionRunner(ruleSuite, name, renderOptions)

}

trait StripResultTypesFunction {

  /**
   * Stores only the ruleResult, removing the structure including the resultDDL column
   *
   * @param expressionResults
   * @return
   */
  def strip_result_ddl(expressionResults: Column): Column =
    column( StripResultTypes(expression(expressionResults)) )
}
