package com.sparkutils.quality.impl.mapLookup

import com.sparkutils.quality.impl.mapLookup.MapLookupFunctions.MapLookups
import org.apache.spark.sql.{Column, ShimUtils}
import org.apache.spark.sql.functions.col

object MapLookup {
  /**
   * For withColumn / select usage, the map generation and lookup expressions must be of the same type
   */
  def apply(mapLookupName: Column, lookupKey: Column, mapLookups: MapLookups): Column =
    ShimUtils.callFunction("map_lookup", mapLookupName, lookupKey, col(mapLookups))

}
