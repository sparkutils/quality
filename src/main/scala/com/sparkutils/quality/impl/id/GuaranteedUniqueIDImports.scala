package com.sparkutils.quality.impl.id

import org.apache.spark.sql.Column

trait GuaranteedUniqueIDImports {
  /**
   * Creates a uniqueID backed by the GuaranteedUniqueID Spark Snowflake ID approach
   */
  def uniqueID(prefix: String): Column =
    new Column(GuaranteedUniqueIdIDExpression(
      GuaranteedUniqueID() // defaults are all fine, ms just relates to definition instead of action
      , prefix
    ))
}
