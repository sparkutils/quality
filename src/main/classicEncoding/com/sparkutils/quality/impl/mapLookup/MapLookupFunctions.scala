package com.sparkutils.quality.impl.mapLookup

import com.sparkutils.quality.impl.RuleRegistrationFunctions.registerWithChecks
import com.sparkutils.quality.impl.util.{Config, ConfigFactory}
import com.sparkutils.shim.toCatalyst
import org.apache.spark.sql.catalyst.expressions.{Expression, IsNotNull}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, MapData}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql._


import scala.collection.JavaConverters._
import scala.collection.Map

object MapLookupFunctions {

  /**
   * Used as a param to load the map lookups - note the type of the broadcast is always Map[AnyRef, AnyRef]
   */
  type MapLookups = Map[String, (MapData, DataType)]

  type MapCreator = () => (DataFrame, Column, Column)

  /**
   *  Noop on classic (pre 0.2.0 4.0)
   */
  protected[quality] def registerMapLookupsForAgnostic(registerFunction: (String, Seq[Expression] => Expression) => Unit): Unit = {}

  /**
   * On classic (pre 0.2.0 4.0) the map_lookup and map_contains functions are bound to this MapLookups instance
   * @param mapLookups
   */
  def registerMapLookupsAndFunction(mapLookups: MapLookups) {
    val funcReg = {
      val f = ShimUtils.registerFunction(SparkSession.getActiveSession.get) _
      ((n: String, builder: scala.collection.immutable.Seq[Expression] => Expression) =>
        f(n, s => builder(s.toIndexedSeq)))
    }

    def register(name: String, argsf: scala.Seq[Expression] => Expression, paramNumbers: Set[Int] = Set.empty, minimum: Int = -1) =
      registerWithChecks(funcReg, name, argsf, paramNumbers, minimum)

    val f = (exps: Seq[Expression]) => MapLookup(exps(0), exps(1), mapLookups)
    register("map_lookup", f, Set(2))

    val sf = (exps: Seq[Expression]) => IsNotNull(  MapLookup(exps(0), exps(1), mapLookups) )
    register("map_contains", sf, Set(2))
  }

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
    }.toMap

  private def mapFromDF(id: String, df: DataFrame, key: Column, value: Column) = {
    val translated = df.select(key.as("key"), value.as("value"))
    val map = translated.toLocalIterator().asScala.map {
      mapPair =>
        toCatalyst(mapPair.get(0)) ->
          toCatalyst(mapPair.get(1))
    }.toMap

    val mapData: MapData = ArrayBasedMapData(map)
    id -> (mapData, translated.schema.last.dataType)
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
