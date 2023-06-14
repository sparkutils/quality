package com.sparkutils.quality.impl.mapLookup

import com.sparkutils.quality.impl.util.{Config, ConfigFactory, Row}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.{Expression, IsNotNull}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, MapData}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, DataFrame, Encoder, Encoders, QualitySparkUtils, SparkSession}

import scala.collection.JavaConverters._

object MapLookupFunctions {

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
  def mapLookupsFromDFs(creators: Map[String, MapCreator]): MapLookups =
    creators.map{
      case (id, mapCreator: MapCreator) =>
        val (df, key, value) = mapCreator()

        mapFromDF(id, df, key, value)
    }

  private def mapFromDF(id: String, df: DataFrame, key: Column, value: Column) = {
    val translated = df.select(key.as("key"), value.as("value"))
    val map = translated.toLocalIterator().asScala.map {
      mapPair =>
        CatalystTypeConverters.convertToCatalyst(mapPair.get(0)) -> CatalystTypeConverters.convertToCatalyst(mapPair.get(1))
    }.toMap

    val mapData: MapData = ArrayBasedMapData(map)
    id -> (df.sparkSession.sparkContext.broadcast(mapData), translated.schema.last.dataType)
  }

  implicit val factory =
    new ConfigFactory[MapConfig, MapRow] {
      override def create(base: Config, row: MapRow): MapConfig =
        MapConfig(base.name, base.source, row.key, row.value)
    }

  implicit val mapRowEncoder: Encoder[MapRow] = Encoders.product[MapRow]

  def loadMaps(configs: Seq[MapConfig]): MapLookups =
    configs.map{
      config =>
        val df = config.source.fold(identity, SparkSession.active.sql(_))

        mapFromDF(config.name, df, expr(config.key), expr(config.value))
    }.toMap

}

/**
 * Represents a configuration row for view loading
 * @param name the view name, this will be used to manage dependencies
 * @param source either a loaded DataFrame or an sql to run against the catalog
 */
case class MapConfig(override val name: String, override val source: Either[DataFrame, String], key: String, value: String) extends Config(name, source)

/**
 * Underlying row information converted into a ViewConfig with the following logic:
 *
 * a) if token is specified sql is ignored
 * b) if token is null sql is used
 * c) if both are null the row will not be used
 */
private[mapLookup] case class MapRow(override val name: String, override val token: Option[String],
                                  override val filter: Option[String], override val sql: Option[String], key: String, value: String)
  extends Row(name, token, filter, sql)
