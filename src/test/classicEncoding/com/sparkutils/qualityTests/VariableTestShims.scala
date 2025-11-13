package com.sparkutils.qualityTests

import com.sparkutils.quality.{MapLookups, registerMapLookupsAndFunction => ogRegMaps}
import com.sparkutils.quality.functions.{map_lookup => ogml, map_contains => ogmc}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

/**
 * Support the differences between classic and post 0.2.0 Spark 4 interfaces using
 */
trait VariableTestShims {

  def registerMapLookupsAndFunction(): Unit = registerMapLookupsAndFunction(Map.empty)
  def registerMapLookupsAndFunction(lookups: MapLookups): Unit =
    ogRegMaps(lookups)

  def map_lookupSQL(mapLookupName: String, lookupKey: String): String =
    s"mapLookup('$mapLookupName', $lookupKey)"

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
    s"mapContains('$mapLookupName', $lookupKey)"

  val mapFactor = 200 // 200 is too large on connnect

}
