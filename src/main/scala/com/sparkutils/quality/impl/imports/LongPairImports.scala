package com.sparkutils.quality.impl.imports

import com.sparkutils.quality.impl.UUIDToLongsExpression
import com.sparkutils.quality.impl.longPair.LongPairExpression
import org.apache.spark.sql.Column

trait LongPairImports {

  /**
   * creates a (lower, higher) struct
   * @param lower
   * @param higher
   * @return
   */
  def long_pair(lower: Column, higher: Column): Column =
    new Column( LongPairExpression(lower.expr, higher.expr) )

  /**
   * creates a (lower, higher) struct from a uuid's least and most significant bits
   * @param uuid
   * @return
   */
  def long_pair_from_uuid(uuid: Column): Column =
    new Column ( UUIDToLongsExpression(uuid.expr) )

}
