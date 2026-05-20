package com.sparkutils.quality.impl.util

import com.sparkutils.quality.impl.RuleEngineRunnerUtils.flattenSalience
import com.sparkutils.quality._
import com.sparkutils.quality.impl.{HasOutput, Trigger, Triggers}

import scala.util.Try
// test is too memory intensive for CI
// $COVERAGE-OFF$

object TopLevelBooleanSuiteBuilder {

  def flattenRules(ruleSuite: RuleSuite): Seq[Rule] =
    ruleSuite.ruleSets.flatMap( ruleSet => ruleSet.rules.map(rule => {
      val expr = rule

      expr
    }))

  def build(runner: HasOutput): Unit = {
    import runner._
    val targetBucket = Try(Triggers.getValue(groupProcessorBucketSizeKey, runner.extraConfig, "130").toInt).
      getOrElse(130)
    val triggerPercentFilter = Try(Triggers.getValue(groupProcessorPercentFilter, runner.extraConfig, "0.12").toDouble).
      getOrElse(0.12)

    val grouped = TopLevelBoolean.bucket(triggerRules.zipWithIndex.zip(flattenSalience(ruleSuite)).
      map(t => Trigger(t._1._1, t._1._2, t._2)), targetBucket, triggerPercentFilter)

    val rules = flattenRules(ruleSuite)

    val group =
      grouped.zipWithIndex.foldLeft(Seq.empty[(Rule, RuleSuite)]){
        case (cur, (group, index)) =>

          val filter = Rule(Id(index, 0), ExpressionRule(group.groupFilter.sql), RunOnPassProcessor(index, Id(index, 0),
            OutputExpression(groupedSqlCall(s"rule_suite_from(the_group, ${index + 1}, 0)"))))
          val suite =
            RuleSuite(Id(index + 1, 0), ruleSets = Seq(RuleSet(Id(index,0), rules = group.triggers.map{
              trigger =>
                rules(trigger.index)
            })))

          cur :+ (filter, suite)
      }

    val topRuleSuite =
      RuleSuite(Id(0, 0), ruleSets = Seq(RuleSet(Id(0,0), rules = group.map(_._1))))

    val nested = group.map(_._2)

    val loc = Triggers.getValue(groupProcessorAuditLocation, runner.extraConfig, "./")
    val name = Triggers.getValue(groupProcessorAuditName, runner.extraConfig, runner.getClass.getSimpleName)

    val rsgroup = RuleSuiteGroup(nested :+ topRuleSuite : _*)
    RuleSuiteGroupIOUtils.toFile(rsgroup, s"$loc/$name")
  }

}
// $COVERAGE-ON$
