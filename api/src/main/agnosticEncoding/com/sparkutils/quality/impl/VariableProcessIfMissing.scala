package com.sparkutils.quality.impl

import com.sparkutils.quality.impl.util.VersionSpecificSerializingImports.uniqueName
import org.apache.spark.sql.{Column, ShimUtils, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructType

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
