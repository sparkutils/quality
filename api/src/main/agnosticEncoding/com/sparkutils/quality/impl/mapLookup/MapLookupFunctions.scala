package com.sparkutils.quality.impl.mapLookup

import com.sparkutils.quality.impl.VariableHelper
import com.sparkutils.quality.impl.util.{Config, ConfigFactory}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql._

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._
import scala.collection.Map

object MapLookupFunctions {

  private val nameCounter = new AtomicInteger(0)

  private val GENERATED_NAME_PREFIX = "QUALITY_LOOKUPS_GENERATED_NAME_"

  // only for the current session, so regardless of on driver with static or connect client this works
  private def uniqueName() = GENERATED_NAME_PREFIX + nameCounter.incrementAndGet()

  /**
   * Used as a param to load the map lookups - note the type of the broadcast is always Map[AnyRef, AnyRef]
   */
  type MapLookups = MapTypes.MapLookups

  type MapCreator = () => (DataFrame, Column, Column)

  /**
   * No-op on 0.2.0 4.0
   * @param mapLookups
   * @return
   */
  def registerMapLookupsAndFunction(mapLookups: MapLookups): Unit = {
  }

  /**
   * Loads maps to broadcast, each individual dataframe may have different associated expressions
   *
   * @param creators a map of string id to MapCreator
   * @param stableName uses a stable name for the lookups
   * @return a map of id to broadcast variables needed for exact lookup and mapping checks
   */
  def mapLookupsFromDFs(creators: Map[String, MapCreator], stableName: String): MapLookups =
    buildStruct(creators.map {
      case (id, mapCreator: MapCreator) =>
        val (df, key, value) = mapCreator()

        mapFromDF(id, df, key, value)
    }.toSeq, stableName)


  /**
   *
   * Loads maps to broadcast, each individual dataframe may have different associated expressions.
   * Uses a generated name
   *
   * @param creators a map of string id to MapCreator
   * @return a map of id to broadcast variables needed for exact lookup and mapping checks
   */
  def mapLookupsFromDFs(creators: Map[String, MapCreator]): MapLookups =
    mapLookupsFromDFs(creators, uniqueName())

  private def buildStruct(strs: Seq[(String, String, DataType)], name: String): MapLookups = {
    val struct = "named_struct("+strs.map(_._2).mkString("\n,")+")"
    val ddl = StructType(strs.map{ s =>
      StructField(s._1, s._3)
    }).toDDL
    // Defaults can't have subqueries
    VariableHelper.createVar(name, s"struct<$ddl>", struct)
    name
  }

  private val MAP_NAME = "QualityMapLookup_Temp_"

  private def mapFromDF(id: String, df: DataFrame, key: Column, value: Column) = {
    val translated = df.select(key.as("key"), value.as("value")).selectExpr("map_from_entries(collect_set(struct(key, value))) as themap")
    translated.createOrReplaceTempView(s"`$MAP_NAME$id`")

    (id, s""""$id", (select first(themap) from `$MAP_NAME$id`)""", translated.schema.fields(0).dataType)
  }

  implicit val factory: ConfigFactory[MapConfig, MapRow] =
    new ConfigFactory[MapConfig, MapRow] {
      override def create(base: Config, row: MapRow): MapConfig =
        MapConfig(base.name, base.source, row.key, row.value)
    }

  implicit val mapRowEncoder: Encoder[MapRow] = Encoders.product[MapRow]

  /**
   *
   * @param configs
   * @param stableName Uses a stable name to register the MapLookups
   * @return stableName
   */
  def loadMaps(configs: Seq[MapConfig], stableName: String): MapLookups =
    buildStruct(configs.map {
      config =>
        val df = config.source.fold(identity, SparkSession.active.sql)

        mapFromDF(config.name, df, expr(config.key), expr(config.value))
    }, stableName)

  def loadMaps(configs: Seq[MapConfig]): MapLookups =
    loadMaps(configs, uniqueName())

}
