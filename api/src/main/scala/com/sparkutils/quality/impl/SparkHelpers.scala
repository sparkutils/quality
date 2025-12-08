package com.sparkutils.quality.impl

import frameless.{Injection, NotCatalystNullable}
import com.sparkutils.quality._
import com.sparkutils.quality.impl.util.{Serializing, SerializingShim}

object PackId {

  def packId(anyId: Any): Long = {
    val r = anyId.asInstanceOf[Id]
    (r.id.toLong << 32) | (r.version & 0xffffffffL)
  }

  def unpack(a: Any): Id =
    if (a == null)
      a.asInstanceOf[Id]
    else
      unpack(a.asInstanceOf[Long])

  def unpack(a: Long): Id = {
    val id = a >> 32
    val version = a.toInt
    Id(id.toInt, version) // lookup goes here
  }
}

trait IntEncodersImplicits extends Serializable {

  // RuleResultWithProcessor's are lost in serialization by design, they only make sense in a given run

  /**
   * Converts the DQ results to and from Int and RuleResult's.  Probability is processed by the value / PassedInt
   */
  implicit val ruleResultToInt: Injection[RuleResult, Int] = Injection(
      Serializing.ruleResultToInt
    ,{
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
  implicit val versionedIdNotNullable: NotCatalystNullable[VersionedId] = new NotCatalystNullable[VersionedId] {}

  // try just id first
  implicit val versionedIdTo: Injection[VersionedId, Long] = Injection(
    {
      case Id(id, version) => (id.toLong << 32) | (version & 0xffffffffL)
    },
    PackId.unpack
  )
}

object IdEncoders extends IdEncodersImplicits {

}
