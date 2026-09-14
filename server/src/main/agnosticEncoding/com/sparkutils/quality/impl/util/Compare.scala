package com.sparkutils.quality.impl.util

import org.apache.spark.sql.types.DataType

object Compare {

  /**
   * Compares two types, ignoring nullability of ArrayType, MapType, StructType, and ignoring case
   * sensitivity of field names in StructType.
   */
  def equalsIgnoreCaseAndNullability(from: DataType, to: DataType): Boolean =
    DataType.equalsIgnoreCaseAndNullability(from, to)
}
