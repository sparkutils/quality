package com.sparkutils.qualityTests

import com.sparkutils.quality.{INC_REWRITE_GENEXP_ERR_MSG, Id, LambdaFunctionImpl, registerLambdaFunctions}
import com.sparkutils.qualityTests.mapLookup.TradeTests._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.junit.Test
import org.scalatest.FunSuite

class AggregatesTest extends FunSuite with TestUtils {

  def doMapTest(transform: Dataset[java.lang.Long] => Dataset[java.lang.Long], sql: String): Unit = evalCodeGensNoResolve {
    val factor = 200 // NB 2000 runs in 5m2s with default index scan and replace and map_concat which clearly fails, 4m 51 with index and map merge with expr based add
    val df = transform( (1 until factor).foldLeft( sparkSession.range(1, 20) ) {
      (ndf, i) =>
        ndf.union( sparkSession.range(1, 20) )
    } )

    val mapCountExpr = df.select(expr(sql).as("mapCountExpr"))

    val map = mapCountExpr.head().getAs[Map[Long, Long]]("mapCountExpr")
    println(map)

    assert(19 == map.size, "Should have had the full 1 until 20 items")
    assert( map.forall(_._2 == factor), "all entries should have had factor as the value")
  }

  val mapTestSql = "aggExpr('MAP<LONG, LONG>',1 > 0, mapWith(id, entry -> entry + 1 ), returnSum() )"

  val mapDeprecatedTestSql = "aggExpr(1 > 0, mapWith('MAP<LONG, LONG>', id, entry -> entry + 1 ), returnSum('MAP<LONG, LONG>') )"

  // doesn't ever see the number in the map
  @Test
  def mapTest: Unit =
    doMapTest(identity, mapTestSql)

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
  def evalSumTest: Unit = evalCodeGensNoResolve {
    import sparkSession.implicits._
    val df = sparkSession.range(1, 20).union(sparkSession.range(1,2).map(_ => null.asInstanceOf[Long]))

    val summed = df.select(expr("aggExpr(id % 2 > 0, sumWith(sum -> sum + id), resultsWith( (sum, count) -> sum / count ) )").as("aggExpr"))

    summed.show(1)

    val set = (1 to 20).filter(_ % 2 > 0)

    assert(summed.head().getAs[Double]("aggExpr") == (set.sum / set.size), "aggExpr did not have the correct math")

    val summedNameF = df.select(expr("aggExpr(id % 2 > 0, inc(), meanF() )").as("aggExpr"))

    summedNameF.show(1)

    assert(summedNameF.head().getAs[Double]("aggExpr") == 1.0, "aggExpr did not have the correct math from NameF")

    val summedNameFParam = df.select(expr("aggExpr(id % 2 > 0, inc(id), meanF() )").as("aggExpr"))

    summedNameFParam.show(1)

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
  def mapAggrCountTest: Unit = doMapAggrCountTest(mapCountExpr)

  @Test
  def mapAggrCountDeprecatedTest: Unit = doMapAggrCountTest(mapDeprecatedCountExpr)

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
    assert(expected == res, "expected the counts to match")
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
    assert(expected == res, "expected the math to match")
  }

  def doMapAggrOnePassTest(expr1: Column, expr2: Column): Unit = evalCodeGensNoResolve {
    import sparkSession.implicits._
    val df = simpleTrades.toDF(tradeCols :_ *)

    val summed = df.select(expr1, expr2)

    doMapCountAggr(summed)(t => t._1 + ", "+ t._2)
    doMapSumAggr[Double](summed)
  }

  lazy val mapDecimalExpr = expr("aggExpr('MAP<STRING, DECIMAL(38,18)>', 1 > 0, mapWith(date || ', ' || product, entry -> entry + IF(ccy='CHF', value, value * ccyrate) ), returnSum() )").as("mapSumExpr")

  @Test
  def mapAggrDecimalTest: Unit = doMapAggrSumTest[java.math.BigDecimal](mapDecimalExpr)

  @Test
  def mapAggrOnePassTest: Unit = doMapAggrOnePassTest(mapSumExpr, mapCountExpr)

  @Test
  def mapAggrOnePassDeprecatedTest: Unit = doMapAggrOnePassTest(mapSumDeprecatedExpr, mapDeprecatedCountExpr)

  /**
   * DecimalPrecision means the type specified in sumWith (that of entry and expected return) is analysed
   * as potentially having a different precision.  This explicit test covers that.
   */
  def doDecimalPrecisionTest(sql: String, expected: BigDecimal = BigDecimal(23245.68200000000)): Unit = evalCodeGensNoResolve {
    import sparkSession.implicits._
    val testDF = Seq(("a", BigDecimal.valueOf(0.34)),("b", BigDecimal.valueOf(23245.342))).
      toDF("str","dec")

    val agg = testDF.selectExpr(sql)
    val rows = agg.as[BigDecimal].collect()
    assert(rows.nonEmpty)
    assert(expected == rows(0))
  }

  @Test
  def decimalPrecisionTest = doDecimalPrecisionTest(  "aggExpr('DECIMAL(38,18)', dec IS NOT NULL, sumWith(entry -> dec + entry ), returnSum()) as agg" )

  @Test
  def decimalPrecisionNO_REWRITETest = doDecimalPrecisionTest(  "aggExpr('NO_REWRITE', dec IS NOT NULL, sumWith('DECIMAL(38,18)', entry -> cast( (dec + entry) as DECIMAL(38,18)) ), returnSum('DECIMAL(38,18)')) as agg" )

  //@Test
  //def decimalPrecisionDeprecatedTest = doDecimalPrecisionTest(  "aggExpr(dec IS NOT NULL, sumWith('DECIMAL(38,18)', entry -> cast((dec + entry) as DECIMAL(38,18)) ), returnSum()) as agg" )

  @Test
  def decimalPrecisionIncTest = doDecimalPrecisionTest(  "aggExpr('DECIMAL(38,18)', dec IS NOT NULL, inc(dec), returnSum()) as agg" )

  @Test
  def decimalPrecisionHofTest = {
    val sf = LambdaFunctionImpl("myinc", "entry -> entry + dec", Id(0,3))
    val sf2 = LambdaFunctionImpl("myinc", "(entry, f) -> entry + dec + f", Id(0,3))
    val rf = LambdaFunctionImpl("myretsum", "(sum, count) -> sum", Id(0,3))
    val rf2 = LambdaFunctionImpl("myretsum", "(sum, f, count) -> sum + f", Id(0,3))
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
    doDecimalPrecisionTest(  s"${pre}aggExpr('DECIMAL(38,18)', dec IS NOT NULL, myinc(_()), myretsum(_(), cast(0.0 as DECIMAL(38,18)), _())) ${post}" )
    // (2) test with 1:1 hof
    doDecimalPrecisionTest(  "aggExpr('DECIMAL(38,18)', dec IS NOT NULL, myinc(_()), myretsum(_(), _())) as agg" )
    // (3) test with partial on sum
    doDecimalPrecisionTest(  s"${pre}aggExpr('DECIMAL(38,18)', dec IS NOT NULL, myinc(_(), cast(0.0 as DECIMAL(38,18))), myretsum(_(), _())) ${post}" )
  }

  @Test
  def decimalPrecisionIncExprTest = doDecimalPrecisionTest(  "aggExpr('DECIMAL(38,18)', dec IS NOT NULL, inc(dec + 0), returnSum()) as agg${post}" )

  @Test
  def decimalPrecisionNO_REWRITEIncTest = try {
    doDecimalPrecisionTest(  "aggExpr('NO_REWRITE', dec IS NOT NULL, inc('DECIMAL(38,18)', cast( dec as DECIMAL(38,18))), returnSum('DECIMAL(38,18)')) as agg" )
    fail("Should have thrown " + INC_REWRITE_GENEXP_ERR_MSG)
  } catch {
    case t: Throwable if t.getMessage == INC_REWRITE_GENEXP_ERR_MSG =>
      // passed
    case t: Throwable =>
      fail("Should have thrown " + INC_REWRITE_GENEXP_ERR_MSG +" but threw ", t)
  }

  @Test
  def decimalPrecisionDeprecatedIncTest = doDecimalPrecisionTest(  "aggExpr(dec IS NOT NULL, inc('DECIMAL(38,18)', dec ), returnSum('DECIMAL(38,18)')) as agg" )

}
