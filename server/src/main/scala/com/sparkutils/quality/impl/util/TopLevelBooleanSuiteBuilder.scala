package com.sparkutils.quality.impl.util

import com.sparkutils.quality.impl.RuleEngineRunnerUtils.flattenSalience
import com.sparkutils.quality._
import com.sparkutils.quality.impl.util.ExtraConfig.ConfigMapOps
import com.sparkutils.quality.impl.{Group, GroupLike, HasOutput, Trigger}

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

  def makeGroups(counter: Counter, runner: HasOutput, rules: Seq[Rule], group: GroupLike): (Seq[RuleSuite], RuleSuite) = {
    val index = counter.next()
    val (groups, processedRules) =
      group.payload.fold[(Seq[RuleSuite], Seq[Rule])]{groups =>
        groups.foldLeft((Seq.empty[RuleSuite], Seq.empty[Rule])){
          case (cur, ng) =>
            val (ggroups, grules) = makeGroup(counter, runner, rules, ng)
            (cur._1 ++ ggroups, cur._2 ++ grules)
        }
      }{
        triggers => makeTriggers(rules, triggers)
      }

    val rs =
      RuleSuite(Id(index, 0), ruleSets = Seq(
        RuleSet(Id(index, 0), rules = processedRules)))

    (groups :+ rs, rs)
  }

  def makeGroup(counter: Counter, runner: HasOutput, rules: Seq[Rule], group: GroupLike): (Seq[RuleSuite], Seq[Rule]) = {
    import runner._

    val (suites, ruleSuite) = makeGroups(counter, runner, rules, group)
    val index = counter.next()
    val filter = Rule(Id(index, 0), ExpressionRule(group.groupExpression.sql), RunOnPassProcessor(index, Id(index, 0),
      OutputExpression(groupedSqlCall(s"rule_suite_from(the_group, ${ruleSuite.id.id}, 0)"))))

    (suites, Seq(filter))
  }

  def makeTriggers(rules: Seq[Rule], triggers: Seq[Trigger]): (Seq[RuleSuite], Seq[Rule]) =
    (Seq.empty, triggers.map{
      trigger =>
        rules(trigger.index).copy(expression = ExpressionRule(trigger.expression.sql))
    })

  def build(runner: HasOutput): Unit = {
    import runner._
    val targetParams = TopLevelBoolean.params(runner)

    val grouped = TopLevelBoolean.bucket(triggers(runner), targetParams)

    val rules = flattenRules(ruleSuite)

    val counter = new Counter()

    val (nested, toprules) =
      grouped.foldLeft((Seq.empty[RuleSuite], Seq.empty[Rule])){
        case ((cgroups, ctoprules), group) =>
          val (groups, toprules) =
            makeGroup(counter, runner, rules, group)
          (cgroups ++ groups, ctoprules ++ toprules)
      }

    val topRuleSuite =
      RuleSuite(Id(0, 0), ruleSets = Seq(RuleSet(Id(0,0), rules = toprules)))

    val loc = runner.extraConfig.string(groupProcessorAuditLocation, "./")
    val name = runner.extraConfig.string(groupProcessorAuditName, runner.getClass.getSimpleName)

    val rsgroup = RuleSuiteGroup(nested :+ topRuleSuite : _*)
    RuleSuiteGroupIOUtils.toFile(rsgroup, s"$loc/$name")
  }

}
