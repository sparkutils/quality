package com.sparkutils.quality

import com.sparkutils.quality.impl.aggregates.AggregateFunctionImports
import com.sparkutils.quality.impl.hash.HashRelatedFunctionImports
import com.sparkutils.quality.impl.id.{GenericLongBasedImports, GuaranteedUniqueIDImports}
import com.sparkutils.quality.impl.imports._
import com.sparkutils.quality.impl.mapLookup.MapLookupFunctionImports
import com.sparkutils.quality.impl.rng.RngFunctionImports
import com.sparkutils.quality.impl.util.{ComparableMapsImports, StructFunctionsImport}
import com.sparkutils.quality.impl.yaml.YamlFunctionImports
import org.apache.spark.sql.ShimUtils.callFunction
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

/**
 * Collection of the Quality Spark Expressions for use in select( Column * )
 */
package object functions extends ComparableMapsImports with GuaranteedUniqueIDImports with GenericLongBasedImports
  with StripResultTypesFunction with RuleResultImport with PackIdImports
  with RuleRunnerFunctionImports with RngFunctionImports with LongPairImports
  with HashRelatedFunctionImports with StructFunctionsImport with AggregateFunctionImports with MapLookupFunctionImports
  with YamlFunctionImports {

  /**
   * Compares aPrefix_lower = bPrefix_lower and aPrefix_higher = bPrefix_higher
   * @param aPrefix
   * @param bPrefix
   * @return
   */
  def long_pair_equal(aPrefix: String, bPrefix: String): Column =
    callFunction("long_pair_equal", lit(aPrefix), lit(bPrefix))

  /**
   * Similar to long_pair_equal but against 160 bit ids.
   * @param aPrefix
   * @param bPrefix
   * @return
   */
  def id_equal(aPrefix: String, bPrefix: String): Column =
    callFunction("id_equal", lit(aPrefix), lit(bPrefix))

  /**
   * Converts a lower and higher pair of longs into a uuid string
   * @param lower
   * @param higher
   */
  def as_uuid(lower: Column, higher: Column): Column =
    callFunction("as_uuid", lower, higher)
}
