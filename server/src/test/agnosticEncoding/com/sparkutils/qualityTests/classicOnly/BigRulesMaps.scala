package com.sparkutils.qualityTests.classicOnly

import com.sparkutils.quality._
import com.sparkutils.quality.impl.mapLookup.MapLookupFunctions
import com.sparkutils.qualityTests.classicOnly.BigRulesGen.{genRules1to1, genRulesMap, testFile}
import com.sparkutils.qualityTests.util.ClassicSharedTests
import com.sparkutils.testing.ConnectionType
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

class BigRulesMaps extends ClassicSharedTests with BigRulesBase {

  override val runWith: ConnectionType = com.sparkutils.testing.ClassicOnly

  val useOptimiser: Boolean = true

  test("map rules for comparison") {
    val s = sparkSession
    import s.implicits._

    val d = s.read.option("header",true).csv(testFile(s,outputDir, "_forMaps")).cache
    d.createOrReplaceTempView("mapsource")
    val conf = genRulesMap(s, outputDir).as[(String, String, Int, String)].collect().zipWithIndex.map {
      case ((key, value, salience, filter), index) =>
        (MapRow(1, 1, "m" + index, None,
          filter = None, Some(s"select * from mapsource where $filter"), key, value),
          s"map_contains('m$index', $key, themaps)",
          s"map_lookup('m$index', $key, themaps)",
          salience + index
        )
    }
    val mapConfig =
      loadMapConfigs(new DataFrameLoader {
        override def load(token: String): DataFrame = ???
      }, conf.map(_._1).toSeq.toDF, Id(1, 1))
    MapLookupFunctions.loadMaps(mapConfig._1, "themaps")

    // substitute the map names
    val rd = conf.map(t => (t._2, t._3, t._4)).toSeq.toDS
    val ruleSuite = rules(s, rd)

    val res = doRuleTest(s, ruleSuite,
      "1:1 loaded but should group via TopLevelBooleanGrouper",
      extraConfig = Map(
        showSplitCompilationTime -> "true",
        "statsEvery" -> "1000"
      ))
    val play = res.persist(StorageLevel.OFF_HEAP)

    val remaining = play.filter("(k_out is null) or (k != k_out) or (l != l_out) or (l_out is null)")

    if (remaining.count() != 0) {
      remaining.show()
    }
    remaining.count() shouldBe 0
  }
}
