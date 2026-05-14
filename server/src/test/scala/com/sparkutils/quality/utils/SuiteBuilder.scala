package com.sparkutils.quality.utils

import com.sparkutils.quality.QualityException.qualityException
import com.sparkutils.quality._
import com.sparkutils.quality.impl.RuleEngineRunner
import com.sparkutils.quality.impl.util.{TopLevelBoolean, Trigger}
import org.apache.spark.sql.catalyst.expressions.Expression

object SuiteBuilder {

  def needsProcessor(ruleSet: RuleSet, rule: Rule): Nothing = {
    qualityException(s"You cannot use a RuleEngine, RuleFolder, ExpressionRunner or CollectRunner if any of the rules do not have RunOnPassProcessors set ruleSet ${ruleSet.id}, rule ${rule.id}}")
  }

  def flattenSalience(ruleSuite: RuleSuite): Array[Int] =
    ruleSuite.ruleSets.flatMap( ruleSet => ruleSet.rules.map(rule =>
      rule.runOnPassProcessor match {
        case NoOpRunOnPassProcessor.noOp => needsProcessor(ruleSet, rule)
        case r: RunOnPassProcessor => r.salience
      }
    )).toArray

  def flattenRules(ruleSuite: RuleSuite): Seq[Rule] =
    ruleSuite.ruleSets.flatMap( ruleSet => ruleSet.rules.map(rule => {
      val expr = rule


      expr
    }))

  def build(target: String, newChildren: Seq[Expression], ruleEngineRunner: RuleEngineRunner, targetBucket: Int = 130, triggerPercentFilter: Double = 0.12): Unit = {
    import ruleEngineRunner._
    val grouped = TopLevelBoolean.bucket(newChildren.zipWithIndex.take(triggerCount).zip(flattenSalience(ruleSuite)).
      map(t => Trigger(t._1._1, t._1._2, t._2)), targetBucket, triggerPercentFilter)

    val rules = flattenRules(ruleSuite)

    val group =
      grouped.zipWithIndex.foldLeft(Seq.empty[(Rule, RuleSuite)]){
        case (cur, (group, index)) =>

          val filter = Rule(Id(index, 0), ExpressionRule(group.groupFilter.sql), RunOnPassProcessor(index, Id(index, 0),
            OutputExpression(s"rule_engine_runner(rule_suite_from(the_group, ${index + 1}, 0))"))) // ruleSuite${index+1} rule_suite_from(the_group, ${index + 1}, 0), per map blows heap
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

    /**
    val all = (nested :+ topRuleSuite).map(combined_rows).reduce(_ union _)
    all.write.mode(SaveMode.Overwrite).json("./src/test/resources/"+target)
    */

    val rsgroup = RuleSuiteGroup(nested :+ topRuleSuite : _*)
    RuleSuiteGroupIOUtils.toFile(rsgroup, "./"+target)
  }

}
