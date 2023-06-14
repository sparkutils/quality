package com.sparkutils.quality.impl.mapLookup

import com.sparkutils.quality.{DataFrameLoader, Id}
import com.sparkutils.quality.impl.util.ConfigLoader
import org.apache.spark.sql.{Column, DataFrame}

trait MapLookupImports {

  def registerMapLookupsAndFunction(mapLookups: MapLookups) =
    MapLookupFunctions.registerMapLookupsAndFunction(mapLookups)

  /**
   * Used as a param to load the map lookups - note the type of the broadcast is always Map[AnyRef, AnyRef]
   */
  type MapLookups = MapLookupFunctions.MapLookups

  type MapCreator = MapLookupFunctions.MapCreator

  /**
   * Loads maps to broadcast, each individual dataframe may have different associated expressions
   *
   * @param creators a map of string id to MapCreator
   * @return a map of id to broadcast variables needed for exact lookup and mapping checks
   */
  def mapLookupsFromDFs(creators: Map[String, MapCreator]): MapLookups =
    MapLookupFunctions.mapLookupsFromDFs(creators)

  import MapLookupFunctions.{factory, mapRowEncoder}

  /**
   * Loads map configurations from a given DataFrame for ruleSuiteId.  Wherever token is present loader will be called and the filter optionally applied.
   * @return A tuple of MapConfig's and the names of rows which had unexpected content (either token or sql must be present)
   */
  def loadMapConfigs(loader: DataFrameLoader, viewDF: DataFrame,
                     ruleSuiteIdColumn: Column,
                     ruleSuiteVersionColumn: Column,
                     ruleSuiteId: Id,
                     name: Column,
                     token: Column,
                     filter: Column,
                     sql: Column,
                     key: Column,
                     value: Column
                    ): (Seq[MapConfig], Set[String]) =
    ConfigLoader.loadConfigs[MapConfig, MapRow](
      loader, viewDF,
      ruleSuiteIdColumn,
      ruleSuiteVersionColumn,
      ruleSuiteId,
      name,
      token,
      filter,
      sql,
      key,
      value
    )

  /**
   * Loads map configurations from a given DataFrame.  Wherever token is present loader will be called and the filter optionally applied.
   * @return A tuple of MapConfig's and the names of rows which had unexpected content (either token or sql must be present)
   */
  def loadMapConfigs(loader: DataFrameLoader, viewDF: DataFrame,
                     name: Column,
                     token: Column,
                     filter: Column,
                     sql: Column,
                     key: Column,
                     value: Column
                    ): (Seq[MapConfig], Set[String]) =
    ConfigLoader.loadConfigs[MapConfig, MapRow](
      loader, viewDF,
      name,
      token,
      filter,
      sql,
      key,
      value
    )

  def loadMaps(configs: Seq[MapConfig]): MapLookups =
    MapLookupFunctions.loadMaps(configs)
}
