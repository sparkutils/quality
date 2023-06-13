package com.sparkutils.quality.utils

import com.sparkutils.quality.{Id, LookupIdFunctionImpl, RuleLogicUtils, RuleSuite}
import org.apache.spark.sql.types.StructType

trait LookupIdFunctionsImports {

  def namesFromSchema(schema: StructType): Set[String] = LookupIdFunctions.namesFromSchema(schema)
  /**
   * Use this function to identify which maps / blooms etc. are used by a given rulesuite
   * collects all rules that are using lookup functions but without constant expressions and the list of lookups that are constants.
   *
   */
  def identifyLookups(ruleSuite: RuleSuite): LookupResults = LookupIdFunctions.identifyLookups(ruleSuite)
}
