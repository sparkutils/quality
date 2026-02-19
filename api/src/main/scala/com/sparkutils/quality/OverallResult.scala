package com.sparkutils.quality

import com.sparkutils.quality.RuleSuite.defaultProbablePass

import scala.annotation.tailrec

/**
 * Probability is evaluated at over probablePass percent, defaults to 80% 0.8.
 * Passed until any failure occurs
 */
@SerialVersionUID(1L)
case class OverallResult(probablePass: Double = defaultProbablePass, currentResult: RuleResult = Passed) extends Serializable {
  /**
   * Processes a RuleResult for DQ
   * @param ruleResult
   * @return
   */
  def process(ruleResult: RuleResult): OverallResult = copy(currentResult = OverallResultHelper.inplace(ruleResult, currentResult, probablePass))

  /**
   * Processes a RuleResult for DefaultProcessing, unlike process, processForDefault returns Passed when receiving a Passed and all other values retain the currentResult
   * @param ruleResult
   * @return
   */
  def processForDefault(ruleResult: RuleResult): OverallResult = copy(currentResult = OverallResultHelper.inplaceForDefault(ruleResult, currentResult, probablePass))
}

protected[quality] object OverallResultHelper {
  @tailrec
  protected[quality] def inplace(ruleResult: RuleResult, currentResult: RuleResult, probablePass: Double): RuleResult =
    ruleResult match {
      case Passed | SoftFailed | DisabledRule | IgnoredRule | DefaultRule => currentResult
      case RuleResultWithProcessor(ruleResult, _) => inplace(ruleResult, currentResult, probablePass)
      case Failed => Failed
      case Probability(x) =>
        if (x < probablePass)
          Failed
        else
          currentResult
    }

  @tailrec
  protected[quality] def inplaceForDefault(ruleResult: RuleResult, currentResult: RuleResult, probablePass: Double): RuleResult =
    ruleResult match {
      case Passed => Passed
      case Probability(x) =>
        if (x < probablePass)
          currentResult
        else
          Passed
      case RuleResultWithProcessor(ruleResult, _) => inplaceForDefault(ruleResult, currentResult, probablePass)
      case _ => currentResult
    }

  protected[quality] def inplaceInt(ruleResult: Int, currentResult: Int, probablePass: Double): Int =
    ruleResult match {
      case PassedInt | SoftFailedInt | DisabledRuleInt | IgnoredRuleInt | DefaultRuleInt => currentResult
      case FailedInt => FailedInt
      case x =>
        if (x < (probablePass * PassedInt))
          FailedInt
        else
          currentResult
    }

  protected[quality] def inplaceForDefaultInt(ruleResult: Int, currentResult: Int, probablePass: Double): Int =
    ruleResult match {
      case PassedInt => PassedInt
      case x if x >= (probablePass * PassedInt) =>
        PassedInt
      case _ => currentResult
    }

}
