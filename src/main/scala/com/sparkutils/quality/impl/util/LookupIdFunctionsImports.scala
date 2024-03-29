package com.sparkutils.quality.impl.util

import com.sparkutils.quality.RuleSuite
import org.apache.spark.sql.types.StructType

trait LookupIdFunctionsImports {

  /**
   * Retrieves the names (with parent fields following the . notation for any nested fields) from the schema
   *
   * @param schema
   * @return
   */
  def namesFromSchema(schema: StructType): Set[String] = LookupIdFunctions.namesFromSchema(schema)

  /**
   * Use this function to identify which maps / blooms etc. are used by a given rulesuite
   * collects all rules that are using lookup functions but without constant expressions and the list of lookups that are constants.
   *
   */
  def identifyLookups(ruleSuite: RuleSuite): LookupResults = LookupIdFunctions.identifyLookups(ruleSuite)
}
