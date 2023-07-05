package com.sparkutils.qualityTests.mapLookup

import com.sparkutils.quality._
import functions.map_contains
import com.sparkutils.qualityTests._
import com.sparkutils.qualityTests.mapLookup.TradeTests.{ccyRate, countryCodeCCY, simpleTrades, tradeCols}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, SparkSession, functions}
import org.junit.Test
import org.scalatest.FunSuite

case class Pair(a: Int, b: Int)

object TradeTests {
  type Trade = (String, String, Int, String, Double, String)

  val countryCodeCCY = Seq(("JP", 1, "JPY"),
    ("GB", 2, "GBP"),
    ("US", 2, "USD"),
    ("CH", 1, "CHF")
  )

  val ccyRate = Seq(("JPY", 1000.0),
    ("GBP", 1.2),
    ("USD", 1.6),
    ("CHF", 1.0)
  )

  val simpleTrades = Seq(("1/12/2020", "ETC", 1, "CHF", 1.0, "JP"),
    ("1/12/2020", "OTC", 1, "GBP", 1.0, "GB"),
    ("1/12/2020", "OTC", 2, "USD", 1.2, "US"),
    ("1/12/2020", "OTC", 300, "JPY", 0.09, "CH"),
    ("1/12/2020", "ETC", 2, "USD", 1.2, "JP"),
    ("1/12/2020", "OTC", 4, "CHF", 1.0, "GB"),
    ("1/12/2020", "OTC", 5, "USD", 1.2, "US"),
    ("1/12/2020", "OTC", 6, "CHF", 1.0, "CH")
  )

  val wrongCountryTrade = Seq(("1/12/2020", "OTC", 6, "CHF", 1.0, "CHRISLAND"))

  val tradeCols = Seq("date", "product", "value", "ccy", "ccyrate", "country")
}

object MapLookupTest {

  val structType = StructType( Seq(
    StructField("funnycheck", IntegerType),
    StructField("ccy", StringType)
  ))

  def doTradeLookupTest(lookups: _root_.com.sparkutils.quality.MapLookups, sparkSession: SparkSession) = {
    registerMapLookupsAndFunction(lookups)
    import sparkSession.implicits._
    val df = simpleTrades.toDF(tradeCols: _ *)

    val res = df.select(col("*"), expr("mapLookup('ccyRate', ccy)").as("lookedUpCCYRate"),
      expr("mapLookup('countryCode', country)").as("countrystuff"),
      expr("mapLookup('countryCode', country).ccy").as("countrystuffccy")
    )
    res.show()

    val countryLookup = countryCodeCCY.map(t => t._1 -> new GenericRowWithSchema(Array(t._2, t._3), structType)).toMap
    import scala.collection.JavaConverters._
    val countryRes = res.select("country", "countrystuff").toLocalIterator().asScala.map {
      row =>
        row.get(0) -> row.get(1)
    }.toMap

    assert(countryLookup == countryRes, "Did not get the same lookup results for country")

    val ccyLookup = ccyRate.toMap
    val ccyRes = res.select("ccy", "lookedUpCCYRate").toLocalIterator().asScala.map {
      row =>
        row.get(0) -> row.get(1)
    }.toMap

    assert(ccyLookup == ccyRes, "Did not get the same lookup results for ccy")

    val countryCCYLookup = countryCodeCCY.map(t => t._1 -> t._3).toMap
    val countryCCYRes = res.select("country", "countrystuffccy").toLocalIterator().asScala.map {
      row =>
        row.get(0) -> row.get(1)
    }.toMap

    assert(countryCCYLookup == countryCCYRes, "Did not get the same lookup results for country's nested ccy")
  }

}

class MapLookupTests extends FunSuite with TestUtils {

  import TradeTests._

  def getRef(): MapLookups = {
    import sparkSession.implicits._

    mapLookupsFromDFs(Map(
      "countryCode" -> ( () => {
        val df = countryCodeCCY.toDF("country", "funnycheck", "ccy")
        (df, new Column("country"), functions.expr("struct(funnycheck, ccy)"))
      } ),
      "ccyRate" -> ( () => {
        val df = ccyRate.toDF("ccy", "rate")
        (df, new Column("ccy"), new Column("rate"))
      })
    ))
  }

  @Test
  def lookupTest: Unit = evalCodeGensNoResolve {
    val lookups = getRef()
    MapLookupTest.doTradeLookupTest(lookups, sparkSession)
  }

  @Test
  def setTest: Unit = evalCodeGensNoResolve {
    val lookups = getRef()
    registerMapLookupsAndFunction(lookups)
    import sparkSession.implicits._
    val df = wrongCountryTrade.toDF(tradeCols :_ *)

    val res = df.select(col("*"), expr("mapContains('countryCode', country)").as("doesCountryExist"))
    assert(!res.head.getAs[Boolean]("doesCountryExist"), "CHRISLAND should not exist")
  }

  @Test
  def emptyTest: Unit = evalCodeGensNoResolve {
    import sparkSession.implicits._

    val lookups = mapLookupsFromDFs(Map(
      "empty" -> ( () => {
        val df = sparkSession.emptyDataset[(String, Int, String)].toDF("country", "funnycheck", "ccy")
        (df, new Column("country"), functions.expr("struct(funnycheck, ccy)"))
      } )
    ))

    registerMapLookupsAndFunction(lookups)
    val df = wrongCountryTrade.toDF(tradeCols :_ *)

    val res = df.select(col("*"), expr("mapContains('empty', country)").as("doesCountryExist")).
      filter("doesCountryExist = false")
    assert(res.count == df.count,"all of the rows should be false" )

    // tests both map_contains and lookup
    val res2 = df.select(col("*"), map_contains("empty", col("country"), lookups).as("doesCountryExist")).
      filter("doesCountryExist = false")
    assert(res2.count == df.count,"all of the rows should be false" )
  }

  @Test
  def multiKey: Unit = evalCodeGensNoResolve {
    import sparkSession.implicits._

    val lookups = mapLookupsFromDFs(Map(
      "multi" -> ( () => {
        val df = countryCodeCCY.toDF("country", "funnycheck", "ccy")
        (df, functions.expr("struct(country, funnycheck)"), functions.expr("ccy"))
      } )
    ))

    registerMapLookupsAndFunction(lookups)

    val res = sparkSession.sql("select mapLookup('multi', struct('GB', 2)) res").as[String].collect()
    assert(res.length == 1,"should have found a single match" )
    assert(res.head == "GBP", "should have got the pound")
  }

  @Test
  def taxonomyLookup: Unit = forceInterpreted {
    val orchid = Seq("open","difficult","prized")

    // 1) if a hierarchy is not given whole term is null and default to input (null, null) key
    // 2) if the hierarchy is not null then it forms the first part of the key with item the second part
    // 3) if any key lookup fails the original item should be returned
    // 4) if the item itself is null the return empty attributes
    val data = Seq(
      Item("flowers", "orchid", Seq("open","difficult","prized")),
      Item("flowers", "dandelion", Seq("weed")),
      Item("cars", "ferrari", Seq("fast","compensatory measure")),
      Item("cars", "skoda", Seq("outdated reputation", "drives doesn't it?"))
    )
    import sparkSession.implicits._
    val datadf = data.toDF("hierarchy","item","data")

    val lookups = mapLookupsFromDFs(Map(
      "hierarchy" -> ( () => {
        (datadf, functions.expr("struct(hierarchy, item)"), functions.expr("data"))
      } )
    ))

    registerMapLookupsAndFunction(lookups)

    registerLambdaFunctions(Seq(LambdaFunction("hierarchyLookup",
      s"( hierarchy, item) -> if(item is null, array(), nvl(mapLookup('hierarchy', struct(hierarchy, item)), array(item)))",Id(0,1))))

    def orNull(what: String) =
      if (what eq null) "null" else s"'$what'"

    def testLookup(hierarchy: String, item: String): Seq[String] = {
      val res = sparkSession.sql(s"select hierarchyLookup(${orNull(hierarchy)}, ${orNull(item)}) res").as[Seq[String]].collect()
      assert(res.length == 1, "should have found a single match only")
      res.head
    }

    assert(testLookup(null, "concorde") == Seq("concorde"), "Rule 1, category is null doesn't exist")
    assert(testLookup("planes", "concorde") == Seq("concorde"), "Rule 1, category doesn't exist")
    assert(testLookup("flowers", "rose") == Seq("rose"), "Rule 2, category exists but item doesn't")
    assert(testLookup("flowers", "orchid") == orchid, "Rule 1, category exists but item doesn't")
    assert(testLookup("flowers", null) == Seq(), "Rule 4")

    // NB this only works as there is a struct (tuple) wrapping the fields so the lookup itself is non-null, although the values are null
  }
}

case class Item(hierarchy: String, item: String, attributes: Seq[String])
