package com.sparkutils.quality.impl.imports

import com.sparkutils.quality.impl.UUIDToLongsExpression
import com.sparkutils.quality.impl.longPair.{LongPairExpression, PrefixedToLongPair}
import org.apache.spark.sql.Column
import org.apache.spark.sql.ShimUtils.{column, expression}

trait LongPairImports {

  /**
   * creates a (lower, higher) struct
   * @param lower
   * @param higher
   * @return
   */
  def long_pair(lower: Column, higher: Column): Column =
    column( LongPairExpression(expression(lower), expression(higher)) )

  /**
   * creates a (lower, higher) struct from a uuid's least and most significant bits
   * @param uuid
   * @return
   */
  def long_pair_from_uuid(uuid: Column): Column =
    column ( UUIDToLongsExpression(expression(uuid)) )

  /**
   * Converts a prefixed long pair to lower, higher
   * @param source
   * @param prefix
   * @return
   */
  def prefixed_to_long_pair(source: Column, prefix: String): Column =
    column( PrefixedToLongPair(expression(source), prefix) )
}
