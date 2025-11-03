package com.sparkutils.quality.impl.imports

import com.sparkutils.quality.impl.UUIDToLongsExpression
import com.sparkutils.quality.impl.longPair.{LongPairExpression, PrefixedToLongPair}
import org.apache.spark.sql.{Column, ShimUtils}
import org.apache.spark.sql.ShimUtils.{callFunction, column, expression}
import org.apache.spark.sql.functions.lit

trait LongPairImports {

  /**
   * creates a (lower, higher) struct
   * @param lower
   * @param higher
   * @return
   */
  def long_pair(lower: Column, higher: Column): Column =
    callFunction("long_pair", lower, higher)

  /**
   * creates a (lower, higher) struct from a uuid's least and most significant bits
   * @param uuid
   * @return
   */
  def long_pair_from_uuid(uuid: Column): Column =
    callFunction("long_pair_from_uuid", uuid)

  /**
   * Converts a prefixed long pair to lower, higher
   * @param source
   * @param prefix
   * @return
   */
  def prefixed_to_long_pair(source: Column, prefix: String): Column =
    callFunction("prefixed_To_Long_Pair", lit(prefix), source)
}
