package com.sparkutils.quality.impl.longPair

import org.apache.spark.sql.types.{LongType, StructField, StructType}

object LongPair {
  val structType = StructType(Seq(
    StructField("lower", LongType, false),
    StructField("higher", LongType, false)))
}
