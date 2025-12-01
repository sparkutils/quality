package com.sparkutils.quality.impl

import frameless.{Injection, NotCatalystNullable}
import com.sparkutils.quality._
import com.sparkutils.quality.impl.util.Serializing

trait IntEncodersImplicits extends Serializable {

  // RuleResultWithProcessor's are lost in serialization by design, they only make sense in a given run

  /**
   * Converts the DQ results to and from Int and RuleResult's.  Probability is processed by the value / PassedInt
   */
  implicit val ruleResultToInt: Injection[RuleResult, Int] = Injection(
    {
      case r: RuleResult => Serializing.ruleResultToInt(r)
    },
    {
      case SoftFailedInt => SoftFailed
      case DisabledRuleInt => DisabledRule
      case FailedInt => Failed
      case PassedInt => Passed
      case a: Int => Probability(a.toDouble / PassedInt)
    })

}

object IntEncoders extends IntEncodersImplicits {
}

trait IdEncodersImplicits extends Serializable {
  implicit val versionedIdNotNullable = new NotCatalystNullable[VersionedId] {}

  // try just id first
  implicit val versionedIdTo: Injection[VersionedId, Long] = Injection(
    {
      case Id(id, version) => ((id.toLong) << 32) | (version & 0xffffffffL)
    },
    {
      case a: Long =>
        PackId.unpack(a)
    })
}

object IdEncoders extends IdEncodersImplicits {

}
