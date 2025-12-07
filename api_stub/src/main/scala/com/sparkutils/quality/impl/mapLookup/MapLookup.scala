package com.sparkutils.quality.impl.mapLookup

import com.sparkutils.quality.impl.mapLookup.MapTypes.MapLookups
import org.apache.spark.sql.Column

object MapLookup {
  /**
   * For withColumn / select usage, the map generation and lookup expressions must be of the same type
   */
  def apply(mapLookupName: Column, lookupKey: Column, mapLookups: MapLookups): Column = ???
}
