package com.sparkutils.quality.impl.util

import com.sparkutils.quality.impl.RuleEngineRunnerUtils.flattenSalience
import com.sparkutils.quality._
import com.sparkutils.quality.impl.util.ExtraConfig.ConfigMapOps
import com.sparkutils.quality.impl.{HasOutput, Trigger}

object TopLevelBooleanSuiteBuilder {

  def flattenRules(ruleSuite: RuleSuite): Seq[Rule] =
    ruleSuite.ruleSets.flatMap( ruleSet => ruleSet.rules.map(rule => {
      val expr = rule

      expr
    }))

  def triggers(runner: HasOutput): Seq[Trigger] = {
    import runner._
    triggerRules.zipWithIndex.zip(flattenSalience(ruleSuite)).
      map(t => Trigger(t._1._1, t._1._2, t._2))
  }

  def build(runner: HasOutput): Unit = {
    import runner._
    val targetParams = TopLevelBoolean.params(runner)

    val grouped = TopLevelBoolean.bucket(triggers(runner), targetParams)

    val rules = flattenRules(ruleSuite)

    val group =
      grouped.zipWithIndex.foldLeft(Seq.empty[(Rule, RuleSuite)]){
        case (cur, (group, index)) =>

          val filter = Rule(Id(index, 0), ExpressionRule(group.groupFilter.sql), RunOnPassProcessor(index, Id(index, 0),
            OutputExpression(groupedSqlCall(s"rule_suite_from(the_group, ${index + 1}, 0)"))))
          val suite =
            RuleSuite(Id(index + 1, 0), ruleSets = Seq(RuleSet(Id(index,0), rules = group.triggers.map{
              trigger =>
                rules(trigger.index).copy(expression = ExpressionRule(trigger.expression.sql))
            })))

          cur :+ (filter, suite)
      }

    val topRuleSuite =
      RuleSuite(Id(0, 0), ruleSets = Seq(RuleSet(Id(0,0), rules = group.map(_._1))))

    val nested = group.map(_._2)

    val loc = runner.extraConfig.string(groupProcessorAuditLocation, "./")
    val name = runner.extraConfig.string(groupProcessorAuditName, runner.getClass.getSimpleName)

    val rsgroup = RuleSuiteGroup(nested :+ topRuleSuite : _*)
    RuleSuiteGroupIOUtils.toFile(rsgroup, s"$loc/$name")
  }

}
