package com.sparkutils.quality.impl.util

import org.apache.spark.sql.Column
import org.apache.spark.sql.ShimUtils.{column, expression}
import org.apache.spark.sql.qualityFunctions.utils
import org.apache.spark.sql.types.DataType

trait ComparableMapsImports {
  /**
   * Efficiently converts the map column to struct for comparison, unioning, sorting etc.
   * @param map
   * @param compareF - allows overriding of the default implementation
   * @return
   */
  def comparable_maps(map: Column, compareF: DataType => Option[(Any, Any) => Int] = (dataType: DataType) => utils.defaultMapCompare(dataType)): Column =
    ComparableMapConverter(map,compareF)

  /**
   * Efficiently converts the mapStruct column to it's original Map type
   *
   * @param mapStruct
   * @return
   */
  def reverse_comparable_maps(mapStruct: Column): Column =
    column(ComparableMapReverser(expression(mapStruct)))
}
