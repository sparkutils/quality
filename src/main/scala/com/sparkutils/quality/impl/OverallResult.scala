package com.sparkutils.quality.impl

import com.sparkutils.quality.impl.imports.RuleResultsImports.DefaultRuleInt
import com.sparkutils.quality.{DefaultRule, DisabledRule, DisabledRuleInt, Failed, FailedInt, IgnoredRule, IgnoredRuleInt, Passed, PassedInt, Probability, RuleResult, RuleResultWithProcessor, SoftFailed, SoftFailedInt}

import scala.annotation.tailrec

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
      case _ => currentResult
    }

}
