package com.sparkutils.quality.impl

import com.sparkutils.quality.QualityException.qualityException
import com.sparkutils.quality.impl.RuleRegistrationFunctions.{defaultParseTypes, getString}
import com.sparkutils.quality.impl.util.VersionSpecificSerializingImports.uniqueName
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.{Column, ShimUtils}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructType

object VariableProcessIfMissing {

  protected[quality] def registerProcessIfAttributeMissingForAgnostic(registerFunction: (String, Seq[Expression] => Expression) => Unit): Unit = {
    // parse the rulesuite, call the functions with structs, set a new variable, probably needs to be direct
    registerFunction("process_if_attribute_missing", {
      case Seq(OfRuleSuite(ruleSuite), ddl, name) =>
        val s = defaultParseTypes(getString(ddl, 1)).
          collect {case s:StructType => s}.
          getOrElse(qualityException("process_if_attribute_missing Did not get a struct dll for param 2"))
        val r = com.sparkutils.quality.processIfAttributeMissing(ruleSuite, s)

    })
  }
}

trait VariableProcessIfMissing {
  /**
   * Processes a given RuleSuite to replace any coalesceIfMissingAttributes.  This may be called before validate / docs but
   * *must* be called *before* adding the expression to a dataframe.
   *
   * @param ruleSuite a Column representing a rule suite from register_rule_suite_variable
   * @param schema The names to validate against, if empty no attempt to process coalesceIfAttributeMissing will be made
   * @return
   */
  def process_if_attribute_missing(ruleSuite: Column, schema: StructType = StructType(Seq()), stableName: String) =
    ShimUtils.callFunction("process_if_attribute_missing", ruleSuite, lit(schema.toDDL), lit(stableName))

  /**
   * Processes a given RuleSuite to replace any coalesceIfMissingAttributes.  This may be called before validate / docs but
   * *must* be called *before* adding the expression to a dataframe.
   *
   * @param ruleSuite a Column representing a rule suite from register_rule_suite_variable
   * @param schema The names to validate against, if empty no attempt to process coalesceIfAttributeMissing will be made
   * @return
   */
  def process_if_attribute_missing(ruleSuite: Column, schema: StructType = StructType(Seq())) =
    process_if_attribute_missing(ruleSuite, schema, uniqueName())
}
