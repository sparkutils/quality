package com.sparkutils.quality.impl.mapLookup

trait MapLookupImportImports {

  def registerMapLookupsAndFunction(mapLookups: MapLookups) =
    MapLookupImport.registerMapLookupsAndFunction(mapLookups)

  /**
   * Used as a param to load the map lookups - note the type of the broadcast is always Map[AnyRef, AnyRef]
   */
  type MapLookups = MapLookupImport.MapLookups

  type MapCreator = MapLookupImport.MapCreator

  /**
   * Loads maps to broadcast, each individual dataframe may have different associated expressions
   *
   * @param creators a map of string id to MapCreator
   * @return a map of id to broadcast variables needed for exact lookup and mapping checks
   */
  def mapLookupsFromDFs(creators: Map[String, MapCreator]): MapLookups =
    MapLookupImport.mapLookupsFromDFs(creators)

}
