package com.sparkutils.quality.impl.mapLookup

import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.types.DataType

import scala.collection.Map

object MapTypes {

  /**
   * Used as a param to load the map lookups - note the type of the broadcast is always Map[AnyRef, AnyRef]
   */
  type MapLookups = Map[String, (MapData, DataType)]
}