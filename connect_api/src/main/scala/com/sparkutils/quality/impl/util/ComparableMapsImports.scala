package com.sparkutils.quality.impl.util

import org.apache.spark.sql.{Column, ShimUtils}

trait ComparableMapsImports {
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
