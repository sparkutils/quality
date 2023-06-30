package com.sparkutils.quality

import com.sparkutils.quality.impl.bloom.{BloomExpressionFunctions, BloomFilterLookupFunctionImport}
import com.sparkutils.quality.impl.hash.HashRelatedFunctionImports
import com.sparkutils.quality.impl.id.{GenericLongBasedImports, GuaranteedUniqueIDImports}
import com.sparkutils.quality.impl.imports._
import com.sparkutils.quality.impl.longPair.AsUUID
import com.sparkutils.quality.impl.rng.RngFunctionImports
import com.sparkutils.quality.impl.util.{ComparableMapsImports, StructFunctionsImport}
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{And, EqualTo}

/**
 * Collection of the Quality Spark Expressions for use in select( Column * )
 */
package object functions extends ComparableMapsImports with GuaranteedUniqueIDImports with GenericLongBasedImports
  with BloomFilterLookupFunctionImport with StripResultTypesFunction with RuleResultImport with PackIdImports
  with RuleRunnerFunctionImports with RngFunctionImports with LongPairImports with BloomExpressionFunctions
  with HashRelatedFunctionImports with StructFunctionsImport {

  /**
   * Compares aPrefix_lower = bPrefix_lower and aPrefix_higher = bPrefix_higher
   * @param aPrefix
   * @param bPrefix
   * @return
   */
  def long_pair_equal(aPrefix: String, bPrefix: String): Column = {

    def lower(a: Any) = UnresolvedAttribute(s"${a}_lower")

    def higher(a: Any) = UnresolvedAttribute(s"${a}_higher")

    new Column(And(EqualTo(lower(aPrefix), lower(bPrefix)), EqualTo(higher(aPrefix), higher(bPrefix))))
  }

  /**
   * Similar to long_pair_equal but against 160 bit ids.
   * @param aPrefix
   * @param bPrefix
   * @return
   */
  def id_equal(aPrefix: String, bPrefix: String): Column = {
    def attr(a: Any, field: String) = UnresolvedAttribute(s"${a}_$field")
    val a = aPrefix
    val b = bPrefix

    new Column(
      And(And(EqualTo(attr(a, "base"), attr(b, "base")),
        EqualTo(attr(a, "i0"), attr(b, "i0"))),
        EqualTo(attr(a, "i1"), attr(b, "i1")))
    )
  }

  /**
   * Converts a lower and higher pair of longs into a uuid string
   * @param lower
   * @param higher
   */
  def as_uuid(lower: Column, higher: Column): Column =
    AsUUID(lower, higher)
}
