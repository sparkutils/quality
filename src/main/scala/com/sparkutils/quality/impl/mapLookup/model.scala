package com.sparkutils.quality.impl.mapLookup

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.{Expression, IsNotNull}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, MapData}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, DataFrame, QualitySparkUtils, SparkSession}

import scala.collection.JavaConverters._



object MapLookupImport {

  def registerMapLookupsAndFunction(mapLookups: MapLookups) {
    val funcReg = SparkSession.getActiveSession.get.sessionState.functionRegistry
    val register = QualitySparkUtils.registerFunction(funcReg) _

    val f = (exps: Seq[Expression]) => MapLookup(exps(0), exps(1), mapLookups)
    register("mapLookup", f)

    val sf = (exps: Seq[Expression]) => IsNotNull(  MapLookup(exps(0), exps(1), mapLookups) )
    register("mapContains", sf)
  }

  /**
    * Used as a param to load the map lookups - note the type of the broadcast is always Map[AnyRef, AnyRef]
   */
  type MapLookups = Map[ String, ( Broadcast[MapData], DataType ) ]

  type MapCreator = () => (DataFrame, Column, Column)

  /**
    * Loads maps to broadcast, each individual dataframe may have different associated expressions
   *
    * @param creators a map of string id to MapCreator
    * @return a map of id to broadcast variables needed for exact lookup and mapping checks
    */
  def mapLookupsFromDFs(creators: Map[String, MapCreator]): MapLookups = {
    creators.map{
      case (id, mapCreator: MapCreator) =>
        val (df, key, value) = mapCreator()

        val translated = df.select(key.as("key"), value.as("value"))
        val map = translated.toLocalIterator().asScala.map{
            mapPair =>
              CatalystTypeConverters.convertToCatalyst(mapPair.get(0)) -> CatalystTypeConverters.convertToCatalyst(mapPair.get(1))
          }.toMap

        val mapData: MapData = ArrayBasedMapData(map)
        id -> (df.sparkSession.sparkContext.broadcast(mapData), translated.schema.last.dataType)
    }
  }

}
