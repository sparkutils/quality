package com.sparkutils.quality

import com.sparkutils.quality.impl.RuleSuiteHelpers
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit}

/**
 * Parameterisation over RuleSuite parameters in 0.2.0 generic Spark4+ runners
 */
sealed trait RuleSuiteParam[T] {
  def column(t: T): Column
}

/**
 * Represents a RuleSuite within a named variable RuleSuiteGroup, used with rule_suite_from.
 *
 * Where [[id.version]] is Int.MinValue the translation will be highest RuleSuite with that [[id.id]]
 *
 * @param groupName
 * @param id
 */
case class GroupRuleId(groupName: String, id: Id)

object RuleSuiteParam {

  implicit val rsParam: RuleSuiteParam[RuleSuite] = new RuleSuiteParam[RuleSuite] {
    override def column(t: RuleSuite): Column = lit(RuleSuiteHelpers.serialize(t))
  }

  implicit val varParam: RuleSuiteParam[String] = new RuleSuiteParam[String] {
    override def column(t: String): Column = col(t)
  }

  implicit val colParam: RuleSuiteParam[Column] = new RuleSuiteParam[Column] {
    override def column(t: Column): Column = t
  }

  implicit val idNameParam: RuleSuiteParam[GroupRuleId] = new RuleSuiteParam[GroupRuleId] {
    override def column(t: GroupRuleId): Column =
      if (t.id.version == Int.MinValue)
        rule_suite_from(t.groupName, t.id.id)
      else
        rule_suite_from(t.groupName, t.id.id, t.id.version)
  }

}
