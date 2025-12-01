package com.sparkutils.quality.impl.util

import org.apache.spark.sql.{Column, ShimUtils}
import org.apache.spark.sql.qualityFunctions.utils
import org.apache.spark.sql.types.DataType

trait ComparableMapsImports {
  /**
   * Efficiently converts the map column to struct for comparison, unioning, sorting etc.
   *
   * NOTE THIS VERSION IS CLASSIC ONLY
   *
   * @param map
   * @param compareF - allows overriding of the default implementation
   * @return
   */
  def comparable_maps_classic(map: Column, compareF: DataType => Option[(Any, Any) => Int] = (dataType: DataType) => utils.defaultMapCompare(dataType)): Column =
    ComparableMapConverter(map,compareF)

  /**
   * Efficiently converts the map column to struct for comparison, unioning, sorting etc.
   * @param map
   * @return
   */
  def comparable_maps(map: Column): Column =
    ShimUtils.callFunction("comparable_Maps", map)

  /**
   * Efficiently converts the mapStruct column to it's original Map type
   *
   * @param mapStruct
   * @return
   */
  def reverse_comparable_maps(mapStruct: Column): Column =
    ShimUtils.callFunction("reverse_Comparable_Maps", mapStruct)
}
