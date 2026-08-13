package com.sparkutils.qualityTests.mapLookup

import com.sparkutils.quality.impl.mapLookup.MapConfig
import com.sparkutils.quality.{DataFrameLoader, Id, MapRow, loadMapConfigs, loadMaps}
import com.sparkutils.qualityTests.util.SharedPureConnectTests
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.DataFrame

// NB the other combinations of loading are covered by the ViewLoaderTest
class MapLoaderTest extends SharedPureConnectTests {

  val loader = new DataFrameLoader {
    override def load(token: String): DataFrame = {
      val s = sparkSession
    import s.implicits._
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

  test("testConfigLoading") {
    val s = sparkSession
    import s.implicits._

    val (mapConfigs, _) = loadMapConfigs(loader, config.toDF(), expr("ruleSuiteId"), expr("ruleSuiteVersion"), Id(1,1),
      col("name"),col("token"),col("filter"),col("sql"),col("key"),col("value")
    )

    doConfigTest(mapConfigs)
  }

  test("testConfigLoadingWithoutIds") {
    val s = sparkSession
    import s.implicits._
    def to2(mapRow: MapRow): MapRow2 = MapRow2(mapRow.name, mapRow.token, mapRow.filter, mapRow.sql, mapRow.key, mapRow.value)

    val (mapConfigs, _) = loadMapConfigs(loader, config.map(to2).toDF(),
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

  test("testMapLoading") {
    val s = sparkSession
    import s.implicits._
    val (mapConfigs, _) = loadMapConfigs(loader, config.toDF(), expr("ruleSuiteId"), expr("ruleSuiteVersion"), Id(1,1),
      col("name"),col("token"),col("filter"),col("sql"),col("key"),col("value")
    )

    val maps = loadMaps(mapConfigs)
    MapLookupTest.doTradeLookupTest(maps, sparkSession)
  }

  test("testMapSQLLoading") {
    val s = sparkSession
    import s.implicits._

    TradeTests.ccyRate.toDF("ccy", "rate").createOrReplaceTempView("ccyRate")

    val viewconfig = Seq(MapRow(Id(1,1),"ccyRate", None, None, Some("select * from ccyRate"), "ccy", "rate")) :+ config.last

    val (mapConfigs, _) = loadMapConfigs(loader, viewconfig.toDF(), Id(1,1))

    val maps = loadMaps(mapConfigs)
    MapLookupTest.doTradeLookupTest(maps, sparkSession)
  }

}

case class MapRow2(name: String, token: Option[String], filter: Option[String], sql: Option[String], key: String, value: String)