package com.sparkutils.qualityTests.bloom

import com.sparkutils.quality.impl.bloom.BloomConfig
import com.sparkutils.quality.{DataFrameLoader, Id, loadBloomConfigs, loadBlooms}
import com.sparkutils.qualityTests.TestUtils
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.DataFrame
import org.junit.Test

// NB the other combinations of loading are covered by the ViewLoaderTest
class BloomLoaderTest extends TestUtils {

  val loader = new DataFrameLoader {
    override def load(token: String): DataFrame = {
      import sparkSession.implicits._

      token match {
        case "undertwenty" => sqlContext.range(0, 19)
        case "twenties" => sqlContext.range(20, 29)
      }
    }
  }

  val config =
    Seq(
      BloomRow(Id(1,1),"undertwenty", Some("undertwenty"), None, None, false, "id", 20, 0.01 ),
      BloomRow(Id(1,1),"twenties", Some("twenties"), None, None, true, "id", 10, 0.01 )
    )

  @Test
  def testConfigLoading(): Unit = {
    import sparkSession.implicits._

    val (bloomConfigs, _) = loadBloomConfigs(loader, config.toDF(), expr("id.id"), expr("id.version"), Id(1,1),
      col("name"),col("token"),col("filter"),col("sql"), col("bigBloom"),
      col("value"), col("numberOfElements"), col("expectedFPP")
    )

    doConfigTest(bloomConfigs)
  }

  @Test
  def testConfigLoadingWithoutIds(): Unit = {
    import sparkSession.implicits._

    val (bloomConfigs, _) = loadBloomConfigs(loader, config.map(_.to2).toDF(),
      col("name"),col("token"),col("filter"),col("sql"), col("bigBloom"),
      col("value"), col("numberOfElements"), col("expectedFPP")
    )

    doConfigTest(bloomConfigs)
  }

  private def doConfigTest(bloomConfigs: Seq[BloomConfig]) = {
    assert(bloomConfigs.size == 2)
    assert(bloomConfigs.forall(_.source.isLeft))

    assert(bloomConfigs.forall(_.value == "id"))
    assert(bloomConfigs.forall(_.expectedFPP == 0.01d))
  }

  @Test
  def testBloomLoading(): Unit = {
    import sparkSession.implicits._
    val (bloomConfigs, _) = loadBloomConfigs(loader, config.toDF(), expr("id.id"), expr("id.version"), Id(1,1),
      col("name"),col("token"),col("filter"),col("sql"), col("bigBloom"),
      col("value"), col("numberOfElements"), col("expectedFPP")
    )

    doBloomTest(bloomConfigs)
  }


  @Test
  def testMapSQLLoading(): Unit = {
    import sparkSession.implicits._

    sqlContext.range(0, 19).createOrReplaceTempView("undertwenty")

    val viewconfig = Seq(BloomRow(Id(1,1), "undertwenty", None, None, Some("select * from undertwenty"), false, "id", 20, 0.01 )) :+ config.last

    val (bloomConfigs, _) = loadBloomConfigs(loader, viewconfig.toDF(), expr("id.id"), expr("id.version"), Id(1,1),
      col("name"),col("token"),col("filter"),col("sql"), col("bigBloom"),
      col("value"), col("numberOfElements"), col("expectedFPP")
    )

    doBloomTest(bloomConfigs)
  }

  private def doBloomTest(bloomConfigs: Seq[BloomConfig]) = {
    val blooms = loadBlooms(bloomConfigs)
    val keys = blooms.keySet
    assert(keys == Set("undertwenty", "twenties"))

    val undertwenty = blooms("undertwenty")._1
    val twenties = blooms("twenties")._1

    // these will work at least, longs are in the bloom, not ints
    assert((1L until 19L).map(undertwenty.mightContain(_)).forall(_ == true))
    assert((1 until 19).map(undertwenty.mightContain(_)).forall(_ == false))

    assert((20L until 29L).map(twenties.mightContain(_)).forall(_ == true))
    assert((20 until 29).map(twenties.mightContain(_)).forall(_ == false))
  }
}

case class BloomRow(id: Id, name: String, token: Option[String], filter: Option[String], sql: Option[String], bigBloom: Boolean, value: String, numberOfElements: Long, expectedFPP: Double){
  def to2: BloomRow2 = BloomRow2(name, token, filter, sql, bigBloom, value, numberOfElements, expectedFPP)
}
case class BloomRow2(name: String, token: Option[String], filter: Option[String], sql: Option[String], bigBloom: Boolean, value: String, numberOfElements: Long, expectedFPP: Double)