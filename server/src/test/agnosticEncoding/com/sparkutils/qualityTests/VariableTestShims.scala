package com.sparkutils.qualityTests

import com.sparkutils.quality.MapLookups
import com.sparkutils.quality.classicFunctions.{map_lookup => ogml, map_contains => ogmc}
import org.apache.spark.sql.Column

/**
 * Support the differences between classic and post 0.2.0 Spark 4 interfaces using
 */
trait VariableTestShims {

  def registerMapLookupsAndFunction(): Unit = ()

  var currentLookups: MapLookups = _

  def registerMapLookupsAndFunction(lookups: MapLookups): Unit = {
    currentLookups = lookups
  }

  def map_lookupSQL(mapLookupName: String, lookupKey: String): String =
    s"mapLookup('$mapLookupName', $lookupKey, $currentLookups)"

  def map_lookup(mapLookupName: String, lookupKey: Column, mapLookups: MapLookups): Column =
    ogml(mapLookupName, lookupKey, mapLookups)

  /**
   * Tests if there is a stored value from a map via the name mapLookupName and 'key' lookupKey.  Implementation is map_lookup.isNotNull
   * @param mapLookupName
   * @param lookupKey
   * @param mapLookups
   * @return
   */
  def map_contains(mapLookupName: String, lookupKey: Column, mapLookups: MapLookups): Column =
    ogmc(mapLookupName, lookupKey, mapLookups)

  def map_containsSQL(mapLookupName: String, lookupKey: String): String =
    s"mapContains('$mapLookupName', $lookupKey, $currentLookups)"

  val mapFactor = 10 // 200 is too large on connect

  val idRange = 500 // 6000 was original, but it takes a while, connect has to drag everything to the client

  val aggregatesTestSTooFastBuffer = 4000 // no issue on classic, so we need to make it more expensive for connect due to SPARK-53900
}
