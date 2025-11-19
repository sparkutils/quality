package com.sparkutils.quality.impl

import com.sparkutils.quality.QualityException.qualityException
import com.sparkutils.quality.impl.RuleRegistrationFunctions.{defaultParseTypes, getString}
import com.sparkutils.quality.impl.util.VersionSpecificSerializingImports.uniqueName
import com.sparkutils.quality.{Id, RuleSuite, validate}
import org.apache.spark.sql.catalyst.analysis.FakeSystemCatalog
import org.apache.spark.sql.catalyst.catalog.{TempVariableManager, VariableDefinition}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, VariableReference}
import org.apache.spark.sql.catalyst.util.AttributeNameParser
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.{Column, ShimUtils, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, StructType}

import java.util.Locale

object VariableProcessIfMissing {

  val process_if_attribute_missing_name = "process_if_attribute_missing"

  // if they are we'll blow the stack, there isn't a valid reason for trying to call it as part of a rule
  private def checkNestedCallsInSuite(ruleSuite: RuleSuite): Unit = {

    def checkRuleSuiteHasProcess(rule: String, ruleId: Id, typ: String): Unit =
      if (rule.contains(process_if_attribute_missing_name)) {
        qualityException(s"$process_if_attribute_missing_name should not be used in RuleSuite expressions but was found in $typ with Id $ruleId with text $rule")
      }

    // unlike classic rulesuite calls these will still have rule text
    ruleSuite.ruleSets.foreach(ruleSet => ruleSet.rules.map { rule =>
      rule.expression match {
        case h: HasRuleText =>
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
        com.sparkutils.quality.validate(s, ruleSuite)

        // the resulting rulesuite will have the corrected unresolved expressions already available,
        // the raw sql remains untouched
        val r = com.sparkutils.quality.processIfAttributeMissing(ruleSuite, s)

        // adjust for name sensitivity settings
        val aname = {
          val tn = getString(name, 2)

          // unlike the recursive code, testing setup is not worth it
          // $COVERAGE-OFF$
          val nc =
            if (SQLConf.get.caseSensitiveAnalysis)
              tn
            else
              tn.toLowerCase(Locale.ROOT)
          // $COVERAGE-ON$

          nc
        }
        val varDef = VariableDefinition( Identifier.of(Array("session"), aname), "null",
          Literal.create(RuleSuiteHelpers.serialize(r), BinaryType) )

        val tempVariableManager: TempVariableManager = SparkSession.active.sessionState.catalogManager.tempVariableManager
        val nameParts = AttributeNameParser.parseAttributeName(aname)
        tempVariableManager.create(nameParts, varDef, true)
        VariableReference(nameParts, FakeSystemCatalog, varDef.identifier, varDef)
    })
  }
}

trait VariableProcessIfMissing {

  /**
   * Processes a given RuleSuite to replace any coalesceIfMissingAttributes.  This may be called before validate / docs but
   * *must* be called *before* adding the ruleSuite to a dataframe.
   *
   * @param ruleSuite a Column representing a rule suite from register_rule_suite_variable
   * @param schema    The names to validate against, if empty no attempt to process coalesceIfAttributeMissing will be made
   * @return
   */
  def process_if_attribute_missing_col(ruleSuite: Column, schema: StructType, stableName: String): Column =
    ShimUtils.callFunction("process_if_attribute_missing", ruleSuite, lit(schema.toDDL), lit(stableName))

  /**
   * Processes a given RuleSuite to replace any coalesceIfMissingAttributes.  This may be called before validate / docs but
   * *must* be called *before* adding the ruleSuite to a dataframe.
   *
   * @param ruleSuite a Column representing a rule suite from register_rule_suite_variable
   * @param schema    The names to validate against, if empty no attempt to process coalesceIfAttributeMissing will be made
   * @return
   */
  def process_if_attribute_missing(ruleSuite: Column, schema: StructType, stableName: String): String = {
    // call count for side effect without transferring binary to client
    SparkSession.active.sql("select 1").select( process_if_attribute_missing_col(ruleSuite, schema, stableName) ).count()
    stableName
  }

  /**
   * Processes a given RuleSuite to replace any coalesceIfMissingAttributes.  This may be called before validate / docs but
   * *must* be called *before* adding the ruleSuite to a dataframe.
   *
   * @param ruleSuite a Column representing a rule suite from register_rule_suite_variable
   * @param schema    The names to validate against, if empty no attempt to process coalesceIfAttributeMissing will be made
   * @return
   */
  def process_if_attribute_missing(ruleSuite: Column, schema: StructType): String =
    process_if_attribute_missing(ruleSuite, schema, uniqueName())


  /**
   * Processes a given RuleSuite to replace any coalesceIfMissingAttributes.  This may be called before validate / docs but
   * *must* be called *before* adding the ruleSuite to a dataframe.
   *
   * @param ruleSuite a Column representing a rule suite from register_rule_suite_variable
   * @return
   */
  def process_if_attribute_missing(ruleSuite: Column): String =
    process_if_attribute_missing(ruleSuite, StructType(Seq()))
}
