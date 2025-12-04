package com.sparkutils.quality.impl

import com.sparkutils.quality
import com.sparkutils.quality.QualityException.qualityException
import com.sparkutils.quality.impl.RuleRegistrationFunctions.{defaultParseTypes, getString}
import com.sparkutils.quality.{Id, NoOpRunOnPassProcessor, RuleSuite}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.ShimUtils
import org.apache.spark.sql.types.{BinaryType, StructType}

object VariableProcessIfMissingFunctions {

  val process_if_attribute_missing_name = "process_if_attribute_missing"

  // if they are we'll blow the stack, there isn't a valid reason for trying to call it as part of a rule
  private def checkNestedCallsInSuite(ruleSuite: RuleSuite): Unit = {

    def checkRuleSuiteHasProcess(rule: String, ruleId: Id, typ: String): Unit =
      if (rule.toLowerCase.contains(process_if_attribute_missing_name)) {
        qualityException(s"$process_if_attribute_missing_name should not be used in RuleSuite expressions but was found in $typ with Id $ruleId with text $rule")
      }

    // unlike classic rulesuite calls these will still have rule text
    ruleSuite.ruleSets.foreach(ruleSet => ruleSet.rules.map { rule =>
      rule.expression match {
        case h: quality.HasRuleText =>
          checkRuleSuiteHasProcess(h.rule, rule.id, "trigger rule")
      }
      if (rule.runOnPassProcessor ne NoOpRunOnPassProcessor.noOp) {
        checkRuleSuiteHasProcess(rule.runOnPassProcessor.rule, rule.runOnPassProcessor.id, "output expression")
      }
    })
    ruleSuite.lambdaFunctions.foreach{
      l =>
        checkRuleSuiteHasProcess(l.rule, l.id, "lambda function")
    }
  }

  protected[quality] def registerProcessIfAttributeMissingForAgnostic(registerFunction: (String, Seq[Expression] => Expression) => Unit): Unit = {
    // parse the rulesuite, call the functions with structs, set a new variable, probably needs to be direct
    registerFunction(process_if_attribute_missing_name, {
      case Seq(OfRuleSuite(ruleSuite), ddl, name) =>
        val s = defaultParseTypes(getString(ddl, 1)).
          collect {case s:StructType => s}.
          getOrElse(qualityException(process_if_attribute_missing_name + " Did not get a struct dll for param 2"))

        checkNestedCallsInSuite(ruleSuite)

        // call for side effect
        com.sparkutils.quality.classicFunctions.validate(s, ruleSuite)

        // the resulting rulesuite will have the corrected unresolved expressions already available,
        // the raw sql remains untouched
        val r = com.sparkutils.quality.classicFunctions.processIfAttributeMissing(ruleSuite, s)

        ShimUtils.createVariable(getString(name, 2), Literal.create(RuleSuiteHelpers.serialize(r), BinaryType), true)
    })
  }
}
