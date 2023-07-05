package com.sparkutils.qualityTests.mapLookup

import com.sparkutils.quality.impl.mapLookup.MapConfig
import com.sparkutils.quality.{DataFrameLoader, Id, loadMapConfigs, loadMaps}
import com.sparkutils.qualityTests.TestUtils
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.DataFrame
import org.junit.Test

// NB the other combinations of loading are covered by the ViewLoaderTest
class MapLoaderTest extends TestUtils {

  val loader = new DataFrameLoader {
    override def load(token: String): DataFrame = {
      import sparkSession.implicits._
      token match {
        case "ccyRate" => TradeTests.ccyRate.toDF("ccy", "rate")
        case "countryCode" => TradeTests.countryCodeCCY.toDF("country", "funnycheck", "ccy")
      }
    }
  }

  val config =
    Seq(
      MapRow(Id(1,1),"ccyRate", Some("ccyRate"), None, None, "ccy", "rate"),
      MapRow(Id(1,1),"countryCode", Some("countryCode"), None, None, "country", "struct(funnycheck, ccy)")
    )

  @Test
  def testConfigLoading(): Unit = {
    import sparkSession.implicits._

    val (mapConfigs, _) = loadMapConfigs(loader, config.toDF(), expr("id.id"), expr("id.version"), Id(1,1),
      col("name"),col("token"),col("filter"),col("sql"),col("key"),col("value")
    )

    doConfigTest(mapConfigs)
  }

  @Test
  def testConfigLoadingWithoutIds(): Unit = {
    import sparkSession.implicits._

    val (mapConfigs, _) = loadMapConfigs(loader, config.map(_.to2).toDF(),
      col("name"),col("token"),col("filter"),col("sql"),col("key"),col("value")
    )

    doConfigTest(mapConfigs)
  }

  private def doConfigTest(mapConfigs: Seq[MapConfig]) = {
    assert(mapConfigs.size == 2)
    assert(mapConfigs.forall(_.source.isLeft))

    val sorted = mapConfigs.sortBy(_.name)
    assert(sorted(0).value == "rate")
    assert(sorted(1).key == "country")
  }

  @Test
  def testMapLoading(): Unit = {
    import sparkSession.implicits._
    val (mapConfigs, _) = loadMapConfigs(loader, config.toDF(), expr("id.id"), expr("id.version"), Id(1,1),
      col("name"),col("token"),col("filter"),col("sql"),col("key"),col("value")
    )

    val maps = loadMaps(mapConfigs)
    val keys = maps.keySet
    assert(keys == Set("ccyRate", "countryCode"))

    MapLookupTest.doTradeLookupTest(maps, sparkSession)
  }


  @Test
  def testMapSQLLoading(): Unit = {
    import sparkSession.implicits._

    TradeTests.ccyRate.toDF("ccy", "rate").createOrReplaceTempView("ccyRate")

    val viewconfig = Seq(MapRow(Id(1,1),"ccyRate", None, None, Some("select * from ccyRate"), "ccy", "rate")) :+ config.last

    val (mapConfigs, _) = loadMapConfigs(loader, viewconfig.toDF(), expr("id.id"), expr("id.version"), Id(1,1),
      col("name"),col("token"),col("filter"),col("sql"),col("key"),col("value")
    )

    val maps = loadMaps(mapConfigs)
    val keys = maps.keySet
    assert(keys == Set("ccyRate", "countryCode"))

    MapLookupTest.doTradeLookupTest(maps, sparkSession)
  }

}

case class MapRow(id: Id, name: String, token: Option[String], filter: Option[String], sql: Option[String], key: String, value: String){
  def to2: MapRow2 = MapRow2(name, token, filter, sql, key, value)
}
case class MapRow2(name: String, token: Option[String], filter: Option[String], sql: Option[String], key: String, value: String)