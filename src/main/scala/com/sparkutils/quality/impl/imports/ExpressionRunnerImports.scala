package com.sparkutils.quality.impl.imports

import com.sparkutils.quality.RuleSuite
import com.sparkutils.quality.impl.ExpressionRunner

trait ExpressionRunnerImports {

  /**
   * Runs the ruleSuite expressions saving results as a tuple of (ruleResult: String, resultType: String)
   * @param ruleSuite
   * @param name
   * @return
   */
  def expressionRunner(ruleSuite: RuleSuite, name: String = "expressionResults") =
    ExpressionRunner(ruleSuite, name)

}
