package com.sparkutils.quality

import scala.annotation.tailrec

/**
 * Probability is evaluated at over probablePass percent, defaults to 80% 0.8.
 * Passed until any failure occurs
 */
@SerialVersionUID(1L)
case class OverallResult(probablePass: Double = 0.8, currentResult: RuleResult = Passed) extends Serializable {
  def process(ruleResult: RuleResult): OverallResult = copy(currentResult = OverallResultHelper.inplace(ruleResult, currentResult, probablePass))
}

protected[quality] object OverallResultHelper {
  @tailrec
  protected[quality] def inplace(ruleResult: RuleResult, currentResult: RuleResult, probablePass: Double): RuleResult =
    ruleResult match {
      case Passed | SoftFailed | DisabledRule => currentResult
      case RuleResultWithProcessor(ruleResult, _) => inplace(ruleResult, currentResult, probablePass)
      case Failed => Failed
      case Probability(x) =>
        if (x < probablePass)
          Failed
        else
          currentResult
    }

  protected[quality] def inplaceInt(ruleResult: Int, currentResult: Int, probablePass: Double): Int =
    ruleResult match {
      case PassedInt | SoftFailedInt | DisabledRuleInt => currentResult
      case FailedInt => FailedInt
      case x =>
        if (x < (probablePass * PassedInt))
          FailedInt
        else
          currentResult
    }

}
