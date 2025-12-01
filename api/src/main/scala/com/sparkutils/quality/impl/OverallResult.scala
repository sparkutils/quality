package com.sparkutils.quality.impl

import com.sparkutils.quality.{DisabledRule, DisabledRuleInt, Failed, FailedInt, Passed, PassedInt, Probability, RuleResult, RuleResultWithProcessor, SoftFailed, SoftFailedInt}

import scala.annotation.tailrec

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
