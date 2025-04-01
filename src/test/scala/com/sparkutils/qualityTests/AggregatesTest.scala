package com.sparkutils.qualityTests

import com.sparkutils.quality.{Id, LambdaFunction, registerLambdaFunctions}
import com.sparkutils.quality.impl.RuleRegistrationFunctions.INC_REWRITE_GENEXP_ERR_MSG
import com.sparkutils.quality.functions._
import com.sparkutils.quality.impl.aggregates.ResultsExpression
import com.sparkutils.qualityTests.mapLookup.TradeTests._
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{col, concat, expr, lit, struct, when}
import org.apache.spark.sql.types.{DataType, DecimalType, LongType, MapType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, functions}
import org.junit.Test
import org.scalatest.FunSuite
import org.scalatest.Matchers.convertToAnyShouldWrapper

class AggregatesTest extends FunSuite with TestUtils {

  def doMapTest(transform: Dataset[java.lang.Long] => Dataset[java.lang.Long], sql: String): Unit = evalCodeGensNoResolve {
    val factor = 200 // NB 2000 runs in 5m2s with default index scan and replace and map_concat which clearly fails, 4m 51 with index and map merge with expr based add
    val df = transform( (1 until factor).foldLeft( sparkSession.range(1, 20) ) {
      (ndf, i) =>
        ndf.union( sparkSession.range(1, 20) )
    } )

    val mapCountExpr = df.select(expr(sql).as("mapCountExpr"))

    val map = mapCountExpr.head().getAs[Map[Long, Long]]("mapCountExpr")
    debug(println(map))

    assert(19 == map.size, "Should have had the full 1 until 20 items")
    assert( map.forall(_._2 == factor), "all entries should have had factor as the value")
  }
  val mapTestSql = "aggExpr('MAP<LONG, LONG>',1 > 0, mapWith(id, entry -> entry + 1 ), returnSum() )"

  val mapDeprecatedTestSql = "aggExpr(1 > 0, mapWith('MAP<LONG, LONG>', id, entry -> entry + 1 ), returnSum('MAP<LONG, LONG>') )"


  // doesn't ever see the number in the map
  @Test
  def mapTest: Unit =
    doMapTest(identity, mapTestSql)

  val phoneData = Seq(
    PhoneStuff(1, 1, "iphone4", "ios", "celluer1"),
    PhoneStuff(1, 2, "oppo", "android", "celluer2"),
    PhoneStuff(1, 3, "vivo", "android", "celluer2"),
    PhoneStuff(1, 4, "pixel", "android", "celluer3"),
    PhoneStuff(1, 5, "iphone6", "ios", "celluer3"),
    PhoneStuff(2, 3, "iphone4", "ios", "celluer1"),
    PhoneStuff(2, 3, "oppo", "ios", "celluer1"),
    PhoneStuff(2, 3, "vivo", "ios", "celluer4"),
    PhoneStuff(2, 1, "pixel", "android", "celluer3"),
    PhoneStuff(2, 1, "pixel", "android", "celluer3"),
    PhoneStuff(2, 2, "iphone6", "ios", "celluer4"),
    PhoneStuff(1, 1, "iphone4", "ios", "celluer1")
  )

  @Test
  def multiGroups(): Unit = evalCodeGens {
    import sparkSession.implicits._

    // register the various Quality sql functions as used below
    com.sparkutils.quality.registerQualityFunctions()
    val data = phoneData.toDS()

    def uniqueSub(additionalGroup: String, resultsExpression: ResultsExpression): Column =
      agg_expr(MapType(
        StringType,
        LongType
      ), lit(1L) > lit(0L),
        map_with( functions.col(additionalGroup), col => col + lit(1L)), resultsExpression)

    val resDF =
      data.groupBy("hr","user").agg(
        uniqueSub("mobile", return_sum).alias("mobile_count"),
        uniqueSub("mobile_type", return_sum).alias("mobile_type_count"),
        uniqueSub("sim_type", return_sum).alias("sim_type_count")
      )

    val joined =
      data.join(resDF, Seq("hr", "user")).selectExpr(
        "hr", "user", "mobile", "mobile_type", "sim_type",// "mobile_count", "mobile_type_count", "sim_type_count",
        "mobile_count[mobile] as mobile_count_n",
        "mobile_type_count[mobile_type] as mobile_type_count_n",
        "sim_type_count[sim_type] as sim_type_count_n"
      )

    //joined.show
    val expected = Seq(
      (1L, 1L, "iphone4", "ios", "celluer1", 2L, 2L, 2L),
      (1L, 1L, "iphone4", "ios", "celluer1", 2L, 2L, 2L),
      (1L, 2L, "oppo", "android", "celluer2", 1L, 1L, 1L),
      (1L, 3L, "vivo", "android", "celluer2", 1L, 1L, 1L),
      (1L, 4L, "pixel", "android", "celluer3", 1L, 1L, 1L),
      (1L, 5L, "iphone6", "ios", "celluer3", 1L, 1L, 1L),
      (2L, 1L, "pixel", "android", "celluer3", 2L, 2L, 2L), // ideally these two rows would also show a
      (2L, 1L, "pixel", "android", "celluer3", 2L, 2L, 2L), // unique of 1, different day fun
      (2L, 2L, "iphone6", "ios", "celluer4", 1L, 1L, 1L),
      (2L, 3L, "vivo", "ios", "celluer4", 1L, 3L, 1L),
      (2L, 3L, "oppo", "ios", "celluer1", 1L, 3L, 2L),
      (2L, 3L, "iphone4", "ios", "celluer1", 1L, 3L, 2L)
    ).toDF("hr", "user", "mobile","mobile_type","sim_type","mobile_count_n","mobile_type_count_n","sim_type_count_n")

    joined.count() shouldBe expected.count()
    joined.union(expected).distinct().count() shouldBe expected.distinct().count()
  }

  /**
   * mapWith nested under a groupBy fails before 0.1.3.1
   */
  @Test
  def groupByNestedMapWith(): Unit = evalCodeGens {
    import sparkSession.implicits._

    // register the various Quality sql functions as used below
    com.sparkutils.quality.registerQualityFunctions()
    val data = phoneData.toDS()

    val pureSQL =
      data.selectExpr("hr", "user",
          "explode(map(" +
            "'mobile',mobile," +
            "'mobile_type',mobile_type," +
            "'sim_type',sim_type" +
            "))")
        .groupBy("hr", "user", "key", "value").count()
        .groupBy("hr", "key", "value")
        .agg(
          functions.sum("count").alias("total_count"),
          functions.count("*").alias("unique_count")
        )

    debug( pureSQL.show )

    def uniqueSub(additionalGroup: String, resultsExpression: ResultsExpression): Column =
      agg_expr(MapType(
        StructType(Seq(
          StructField("user", LongType),
          StructField(additionalGroup, StringType))),
        LongType
      ), lit(1L) > lit(0L),
        map_with( functions.struct(functions.col("user"), functions.col(additionalGroup)), col => col + lit(1L)), resultsExpression)

    type RESSUM = (Long, Map[(Long, String), Long], Map[(Long, String), Long], Map[(Long, String), Long])

    val resDF =
      data.groupBy("hr").agg(
        uniqueSub("mobile", return_sum).alias("mobile_count"),
        uniqueSub("mobile_type", return_sum).alias("mobile_type_count"),
        uniqueSub("sim_type", return_sum).alias("sim_type_count")
      )

    val res =
      resDF.as[(Long, Map[(Long, String), Long], Map[(Long, String), Long], Map[(Long, String), Long])].collect()

    def pairs(res: RESSUM, which: RESSUM => Map[(Long, String), Long], name: String) =
      which(res).groupBy(_._1._2).map { mapPairs =>
        (res._1, name, mapPairs._1, mapPairs._2.values.sum, mapPairs._2.keys.size) // total, unique
      }

    val reformattedSUM =
      (res.flatMap { hrPair =>
        pairs(hrPair, _._2, "mobile") ++
        pairs(hrPair, _._3, "mobile_type") ++
        pairs(hrPair, _._4, "sim_type")
      }).toSeq.toDS.toDF("hr", "key", "value", "total_count", "unique_count")

    reformattedSUM.count shouldBe pureSQL.count
    reformattedSUM.union(pureSQL).distinct().count shouldBe pureSQL.count
  }

  // should see lots of the number in the map, was untested under 0.4's
  @Test
  def mapTestSort: Unit =
    doMapTest(_.sort("id"), mapTestSql)

  // doesn't ever see the number in the map
  @Test
  def mapDeprecatedTest: Unit =
    doMapTest(identity, mapDeprecatedTestSql)

  // should see lots of the number in the map, was untested under 0.4's
  @Test
  def mapDeprecatedTestSort: Unit =
    doMapTest(_.sort("id"), mapDeprecatedTestSql)

  @Test
  def sumTestDSL: Unit = evalCodeGensNoResolve {
    import sparkSession.implicits._
    val df = sparkSession.range(1, 20).union(sparkSession.range(1,2).map(_ => null.asInstanceOf[Long]))

    val summed = df.select(agg_expr(LongType, col("id") % 2 > 0, sum_with(sum => sum + col("id")), results_with( (sum, count) => sum / count ) ).as("aggExpr"))

    debug(summed.show(1))

    val set = (1 to 20).filter(_ % 2 > 0)

    assert(summed.head().getAs[Double]("aggExpr") == (set.sum / set.size), "aggExpr did not have the correct math")

    val summedNameF = df.select(agg_expr(LongType, col("id") % 2 > 0, inc, meanf).as("aggExpr"))

    debug(summedNameF.show(1))

    assert(summedNameF.head().getAs[Double]("aggExpr") == 1.0, "aggExpr did not have the correct math from NameF")

    val summedNameFParam = df.select(agg_expr(LongType, col("id") % 2 > 0, inc(col("id")), meanf).as("aggExpr"))

    debug(summedNameFParam.show(1))

    assert(summedNameFParam.head().getAs[Double]("aggExpr") ==  (set.sum / set.size), "aggExpr did not have the correct math from NameFParam")
  }

  @Test
  def evalSumTest: Unit = evalCodeGensNoResolve {
    import sparkSession.implicits._
    val df = sparkSession.range(1, 20).union(sparkSession.range(1,2).map(_ => null.asInstanceOf[Long]))

    val summed = df.select(expr("aggExpr(id % 2 > 0, sumWith(sum -> sum + id), resultsWith( (sum, count) -> sum / count ) )").as("aggExpr"))

    debug(summed.show(1))

    val set = (1 to 20).filter(_ % 2 > 0)

    assert(summed.head().getAs[Double]("aggExpr") == (set.sum / set.size), "aggExpr did not have the correct math")

    val summedNameF = df.select(expr("aggExpr(id % 2 > 0, inc(), meanF() )").as("aggExpr"))

    debug(summedNameF.show(1))

    assert(summedNameF.head().getAs[Double]("aggExpr") == 1.0, "aggExpr did not have the correct math from NameF")

    val summedNameFParam = df.select(expr("aggExpr(id % 2 > 0, inc(id), meanF() )").as("aggExpr"))

    debug(summedNameFParam.show(1))

    assert(summedNameFParam.head().getAs[Double]("aggExpr") ==  (set.sum / set.size), "aggExpr did not have the correct math from NameFParam")
  }

  lazy val mapCountExpr = expr("aggExpr('MAP<STRING, LONG>', 1 > 0, mapWith(date || ', ' || product, entry -> entry + 1 ), resultsWith((sum, count) -> sum ) )").as("mapCountExpr")
  lazy val mapDeprecatedCountExpr = expr("aggExpr(1 > 0, mapWith('MAP<STRING, LONG>', date || ', ' || product, entry -> entry + 1 ), resultsWith('MAP<STRING, LONG>', (sum, count) -> sum ) )").as("mapCountExpr")

  def doMapAggrCountTest(expr: Column): Unit = evalCodeGensNoResolve {
    import sparkSession.implicits._
    val df = simpleTrades.toDF(tradeCols :_ *)

    val summed = df.select(expr)
    doMapCountAggr[String](summed)(t => t._1 + ", "+ t._2)
  }

  @Test
  def mapAggrCountDSLTest: Unit = doMapStructKeyAggrCountTest(mapStructKeyCountDSL)

  @Test
  def mapAggrCountTest: Unit = doMapAggrCountTest(mapCountExpr)

  @Test
  def mapAggrCountDeprecatedTest: Unit = doMapAggrCountTest(mapDeprecatedCountExpr)

  lazy val mapStructKeyCountDSL = agg_expr(MapType(StructType(Seq(StructField("date", StringType), StructField("product", StringType))), LongType), lit(1) > 0,
    map_with(struct(col("date"), col("product")), entry => entry + 1L), return_sum).as("mapCountExpr")
  lazy val mapStructKeyCountExpr = expr("aggExpr('MAP<STRUCT<`date`: STRING, `product`: STRING>, LONG>', 1 > 0, mapWith(struct(date, product), entry -> entry + 1 ), returnSum() )").as("mapCountExpr")
  lazy val mapStructKeyCountDeprecatedExpr = expr("aggExpr(1 > 0, mapWith('MAP<STRUCT<`date`: STRING, `product`: STRING>, LONG>', struct(date, product), entry -> entry + 1 ), returnSum('MAP<STRUCT<`date`: STRING, `product`: STRING>, LONG>') )").as("mapCountExpr")

  val structType = StructType( Seq(
    StructField("date", StringType),
    StructField("product", StringType)
  ))

  def doMapStructKeyAggrCountTest(expr: Column): Unit = evalCodeGensNoResolve {
    import sparkSession.implicits._
    val df = simpleTrades.toDF(tradeCols :_ *)

    val summed = df.select(expr)
    doMapCountAggr[GenericRowWithSchema](summed)(t => new GenericRowWithSchema(Array(t._1, t._2), structType))
  }

  @Test
  def mapStructKeyAggrCountTest: Unit = doMapStructKeyAggrCountTest(mapStructKeyCountExpr)

  @Test
  def mapStructKeyAggrCountDeprecatedTest: Unit = doMapStructKeyAggrCountTest(mapStructKeyCountDeprecatedExpr)

  def doMapCountAggr[T](summed: DataFrame)(group : Trade => T): Unit = {
    val res = summed.head().getAs[Map[T, Long]]("mapCountExpr")

    val expected = simpleTrades.groupBy(group).mapValues( t => t.size)

//    val expected = Map("1/12/2020, ETC" -> 2, "1/12/2020, OTC" -> 6)
    assert(expected.toMap == res, "expected the counts to match")
  }

  lazy val mapSumExpr = expr("aggExpr('MAP<STRING, DOUBLE>', 1 > 0, mapWith(date || ', ' || product, entry -> entry + IF(ccy='CHF', value, value * ccyrate)  ), returnSum() )").as("mapSumExpr")
  lazy val mapSumDeprecatedExpr = expr("aggExpr(1 > 0, mapWith('MAP<STRING, DOUBLE>', date || ', ' || product, entry -> entry + IF(ccy='CHF', value, value * ccyrate)  ), returnSum('MAP<STRING, DOUBLE>') )").as("mapSumExpr")

  trait ToDouble[T] {
    def toDouble(t: T): Double
  }

  implicit object IsDouble extends ToDouble[Double] {
    def toDouble(t: Double): Double = t
  }

  implicit object IsDecimal extends ToDouble[java.math.BigDecimal] {
    def toDouble(t: java.math.BigDecimal): Double = t.doubleValue
  }

  def doMapAggrSumTest[T: ToDouble](expr: Column): Unit = evalCodeGensNoResolve {
    import sparkSession.implicits._
    val df = simpleTrades.toDF(tradeCols :_ *)

    val summed = df.select(expr)
    doMapSumAggr[T](summed)
  }

  @Test
  def mapAggrSumTest: Unit = doMapAggrSumTest[Double](mapSumExpr)

  @Test
  def mapAggrSumDeprecatedTest: Unit = doMapAggrSumTest[Double](mapSumDeprecatedExpr)

  def doMapSumAggr[T: ToDouble](summed: DataFrame): Unit = {
    val i = implicitly[ToDouble[T]]
    import i._
    val res = summed.head().getAs[Map[String, T]]("mapSumExpr").mapValues(toDouble)
    val expected = simpleTrades.groupBy(t => t._1 + ", " + t._2).mapValues( t => t.map( p => if (p._4 == "CHF") p._3.toDouble else p._3 * p._5).sum)

    // val expected = Map("1/12/2020, ETC" -> 3.4, "1/12/2020, OTC" -> 46.4)
    assert(expected.toMap == res.toMap, "expected the math to match")
  }

  def doMapAggrOnePassTest(expr1: Column, expr2: Column): Unit = evalCodeGensNoResolve {
    import sparkSession.implicits._
    val df = simpleTrades.toDF(tradeCols :_ *)

    val summed = df.select(expr1, expr2)

    doMapCountAggr(summed)(t => t._1 + ", "+ t._2)
    doMapSumAggr[Double](summed)
  }

  lazy val mapDecimalDSL = agg_expr(MapType(StringType, DecimalType(38,18)), lit(1) > 0, map_with(concat(col("date"), lit(", "), col("product")),
    entry => entry + when(col("ccy") === "CHF", col("value")).otherwise(col("value") * col("ccyrate")) ), return_sum ).as("mapSumExpr")

  lazy val mapDecimalExpr = // if also works but it's not present in the dsl - expr("aggExpr('MAP<STRING, DECIMAL(38,18)>', 1 > 0, mapWith(date || ', ' || product, entry -> entry + IF(ccy='CHF', value, value * ccyrate) ), returnSum() )").as("mapSumExpr")
    expr("aggExpr('MAP<STRING, DECIMAL(38,18)>', 1 > 0, mapWith(date || ', ' || product, entry -> entry + case when ccy='CHF' then value else value * ccyrate end ), returnSum() )").as("mapSumExpr")

  @Test
  def mapAggrDecimalTest: Unit = doMapAggrSumTest[java.math.BigDecimal](mapDecimalExpr)

  @Test
  def mapAggrDecimalDSLTest: Unit = doMapAggrSumTest[java.math.BigDecimal](mapDecimalDSL)

  @Test
  def mapAggrOnePassTest: Unit = doMapAggrOnePassTest(mapSumExpr, mapCountExpr)

  @Test
  def mapAggrOnePassDeprecatedTest: Unit = doMapAggrOnePassTest(mapSumDeprecatedExpr, mapDeprecatedCountExpr)

  /**
   * DecimalPrecision means the type specified in sumWith (that of entry and expected return) is analysed
   * as potentially having a different precision.  This explicit test covers that.
   */
  def doDecimalPrecisionTest(col: Column, expected: BigDecimal = BigDecimal(23245.68200000000)): Unit = evalCodeGensNoResolve {
    doDecimalPrecisionTestF(df => col, expected = expected)
  }
  def doDecimalPrecisionTestF(col: DataFrame => Column, expected: BigDecimal = BigDecimal(23245.68200000000)): Unit = evalCodeGensNoResolve {
    import sparkSession.implicits._
    val testDF = Seq(("a", BigDecimal.valueOf(0.34)),("b", BigDecimal.valueOf(23245.342))).
      toDF("str","dec")

    val agg = testDF.select(col(testDF))
    val rows = agg.as[BigDecimal].collect()
    assert(rows.nonEmpty)
    assert(expected == rows(0))
  }

  @Test
  def decimalPrecisionTest = doDecimalPrecisionTest(  expr("aggExpr('DECIMAL(38,18)', dec IS NOT NULL, sumWith(entry -> dec + entry ), returnSum()) as agg" ))

  @Test
  def decimalPrecisionExprDSLTest = doDecimalPrecisionTestF( df => agg_expr(DecimalType(38,18), df("dec").isNotNull, sum_with(entry => df("dec") + entry ), return_sum) as "agg")

  @Test
  def decimalPrecisionNO_REWRITETest = doDecimalPrecisionTest(  expr("aggExpr('NO_REWRITE', dec IS NOT NULL, sumWith('DECIMAL(38,18)', entry -> cast( (dec + entry) as DECIMAL(38,18)) ), returnSum('DECIMAL(38,18)')) as agg" ))

  //@Test
  //def decimalPrecisionDeprecatedTest = doDecimalPrecisionTest(  "aggExpr(dec IS NOT NULL, sumWith('DECIMAL(38,18)', entry -> cast((dec + entry) as DECIMAL(38,18)) ), returnSum()) as agg" )

  @Test
  def decimalPrecisionIncTest = doDecimalPrecisionTest(  expr("aggExpr('DECIMAL(38,18)', dec IS NOT NULL, inc(dec), returnSum()) as agg" ))

  @Test
  def decimalPrecisionIncDSLTest = doDecimalPrecisionTestF( df => agg_expr(DecimalType(38,18), df("dec").isNotNull, inc(df("dec")), return_sum) as "agg" )

  @Test
  def decimalPrecisionHofTest = funNRewrites {
    val sf = LambdaFunction("myinc", "entry -> entry + dec", Id(0,3))
    val sf2 = LambdaFunction("myinc", "(entry, f) -> entry + dec + f", Id(0,3))
    val rf = LambdaFunction("myretsum", "(sum, count) -> sum", Id(0,3))
    val rf2 = LambdaFunction("myretsum", "(sum, f, count) -> sum + f", Id(0,3))
    registerLambdaFunctions(Seq(sf, sf2, rf, rf2))
    // NOTE on spark 2.4 it will not auto cast to BigDecimal on part 1 and 3 below
    // as such we wrap sql...
    val (pre, post) = {
      if (sparkVersion != "2.4") // not spark 2.4
        ("","as agg")
      else
        ("cast("," as DECIMAL(38,18)) as agg")
    }

    // (1) test with wider partial application
    doDecimalPrecisionTest( expr( s"${pre}aggExpr('DECIMAL(38,18)', dec IS NOT NULL, myinc(_()), myretsum(_(), cast(0.0 as DECIMAL(38,18)), _())) ${post}" ) )
    // (2) test with 1:1 hof
    doDecimalPrecisionTest( expr("aggExpr('DECIMAL(38,18)', dec IS NOT NULL, myinc(_()), myretsum(_(), _())) as agg" ) )
    // (3) test with partial on sum
    doDecimalPrecisionTest( expr( s"${pre}aggExpr('DECIMAL(38,18)', dec IS NOT NULL, myinc(_(), cast(0.0 as DECIMAL(38,18))), myretsum(_(), _())) ${post}" ) )
  }

  @Test
  def decimalPrecisionIncExprTest = funNRewrites { doDecimalPrecisionTest( expr(  "aggExpr('DECIMAL(38,18)', dec IS NOT NULL, inc(dec + 0), returnSum()) as agg" ) ) }

  @Test
  def decimalPrecisionIncExprDSLTest = funNRewrites { doDecimalPrecisionTestF( df => agg_expr(DecimalType(38,18), df("dec").isNotNull, inc(df("dec") + 0 ), return_sum) as "agg") }

  @Test
  def decimalPrecisionNO_REWRITEIncTest = funNRewrites { try {
    doDecimalPrecisionTest( expr( "aggExpr('NO_REWRITE', dec IS NOT NULL, inc('DECIMAL(38,18)', cast( dec as DECIMAL(38,18))), returnSum('DECIMAL(38,18)')) as agg" ) )
    fail("Should have thrown " + INC_REWRITE_GENEXP_ERR_MSG)
  } catch {
    case t: Throwable if t.getMessage == INC_REWRITE_GENEXP_ERR_MSG =>
      // passed
    case t: Throwable =>
      fail("Should have thrown " + INC_REWRITE_GENEXP_ERR_MSG +" but threw ", t)
  } }

  @Test
  def decimalPrecisionDeprecatedIncTest = funNRewrites { doDecimalPrecisionTest(  expr("aggExpr(dec IS NOT NULL, inc('DECIMAL(38,18)', dec ), returnSum('DECIMAL(38,18)')) as agg" ) ) }

}

case class PhoneStuff(hr: Long, user: Long, mobile: String, mobile_type: String, sim_type: String)
