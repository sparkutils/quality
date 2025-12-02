package com.sparkutils.quality

/**
  * Probability is evaluated at over probablePass percent, defaults to 80% 0.8.
  * Passed until any failure occurs
  */
case class OverallResult(probablePass: Double = 0.8, currentResult: RuleResult = Passed) {
  def process(ruleResult: RuleResult): OverallResult = copy(currentResult = impl.OverallResultHelper.inplace(ruleResult, currentResult, probablePass))
}
