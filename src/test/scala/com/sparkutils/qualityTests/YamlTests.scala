package com.sparkutils.qualityTests

import com.sparkutils.quality._
import com.sparkutils.quality.functions._
import com.sparkutils.quality.impl.YamlDecoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataType
import org.junit.Test
import org.scalatest.FunSuite

import scala.language.postfixOps

class YamlTests extends FunSuite with RowTools with TestUtils {

  def doSerDeTestMaps(original: String, ddl: String) = evalCodeGens {
    def serDe(renderOptions: Map[String, String]) {
      val df = sparkSession.sql(s"select $original bits")
        .select(col("bits"), to_yaml(col("bits"), renderOptions).as("converted"))
        .select(expr("*"), from_yaml(col("converted"), DataType.fromDDL(ddl)).as("deconverted"))

      val r = df.select(col("bits").as("og"), comparable_maps(col("bits")).as("bits"), col("converted"), col("deconverted").as("og_deconverted"), comparable_maps(col("deconverted")).as("deconverted"))
      val filtered = r.filter("deconverted = bits or deconverted is null and bits is null")
      //r.show
      assert(filtered.count == 1)
    }

    serDe(Map.empty)
    serDe(Map("useFullScalarType" -> "true"))
  }

  // CalendarInterval not supported in = / ordering so we need special testing for that

  def doSerDeTestGuess(original: String, ddl: String) = evalCodeGens {
    def serDe(renderOptions: Map[String, String]) {
      val df = sparkSession.sql(s"select $original bits")
        .select(col("bits"), to_yaml(col("bits"), renderOptions).as("converted"))
        .select(expr("*"), from_yaml(col("converted"), DataType.fromDDL(ddl)).as("deconverted"))

      val filtered = df.selectExpr("*", "cast(bits as string) bitsStr", "cast(deconverted as string) deconvertedStr")
        .filter("deconvertedStr = bitsStr or deconvertedStr is null and bitsStr is null")
      //filtered.show
      assert(filtered.count == 1)
    }

    serDe(Map.empty)
    serDe(Map("useFullScalarType" -> "true"))
  }

  @Test
  def structsAsKeys: Unit =
    doSerDeTestMaps("map(named_struct('col1','group', 'col2','parts'), 1235, named_struct('col1','more','col2','parts'), 2666, named_struct('col1',null,'col2','parts'), null)",
      "map<struct<col1: String, col2: String>, long>")

  @Test
  def sequenceAsKeys: Unit =
    doSerDeTestMaps("map(array('col1','group', 'col2','parts'), 1235, array('col1','more','col2','parts'), 2666, array(null,'more',null,'parts'), 2654645666)",
      "map<array<String>, long>")

  @Test
  def structsAsValues: Unit =
    doSerDeTestMaps("map(1235, named_struct('col1','group', 'col2','parts'), 2666, named_struct('col1','more','col2','parts'), 546456, null)",
      "map<long, struct<col1: String, col2: String>>")

  @Test
  def mapsAsValues: Unit =
    doSerDeTestMaps("map(named_struct('col1','group', 'col2','parts'), map(1235, null, 1234, 466), named_struct('col1','more','col2','parts'), map(1235, null, 1234, null), named_struct('col1',null,'col2','parts'), null)",
      "map<struct<col1: String, col2: String>, map<long,long>>")

  @Test
  def sequencesAsValues: Unit =
    doSerDeTestMaps("map(array('col1','group', 'col2','parts'), array('col1','group', 'col2','parts'), array('col1','more','col2','parts'), array('col1','group', 'col2','parts'), array(null,'more',null,'parts'), null)",
      "map<array<String>, array<String>>")

  @Test
  def sequenceAsKeysDecimals: Unit =
    doSerDeTestMaps("map(array('col1','group', 'col2','parts'), cast( 1235.34452 as decimal(38,17)), array('col1','more','col2','parts'), cast( 2666.2345 as decimal(38,17)))",
      "map<array<String>, decimal(38,17)>")

  @Test
  def theRest: Unit = {

    def doSerDe(original: String, ddl: String) = {
      doSerDeTestMaps(original, ddl)
      doSerDeTestMaps(s"named_struct('value', $original)", s"struct<value : $ddl>")
    }

    doSerDe("null", "string")

    doSerDe("\"null\"", "string")

    doSerDe("unbase64('U3BhcmsgU1FM')", "binary")

    doSerDe("true", "boolean")

    doSerDe("53", "byte")

    doSerDe("53", "short")

    doSerDe("5313", "int")

    doSerDe("53133454", "long")

    doSerDe("cast( 53133.454 as float)", "float")

    doSerDe("cast(53133.454 as double)", "double")

    doSerDe("cast(53133454 as timestamp)", "timestamp")

    doSerDe("date('2016-07-30')", "date")

    v3_2_and_above {
      doSerDe("INTERVAL '10' MONTH", "INTERVAL MONTH")

      doSerDe("INTERVAL '10' DAY", "INTERVAL DAY")

      doSerDe("INTERVAL '10' YEAR", "INTERVAL YEAR")
    }

    def doSerDeGuess(original: String, ddl: String) = {
      doSerDeTestGuess(original, ddl)
      doSerDeTestGuess(s"named_struct('value', $original)", s"struct<value : $ddl>")
    }

    not2_4 {
      doSerDeGuess("make_interval(100, 11, 1, 1, 12, 30, 01.001001)", "INTERVAL")
    }

    v3_4_and_above {
      doSerDe("localtimestamp()", "TIMESTAMP_NTZ")
    }
  }

  val UseFullScalarType = "map('useFullScalarType', 'true')"

  @Test
  def decimalViaYaml: Unit = evalCodeGens {
    import sparkSession.implicits._
    val str =
      sparkSession.sql(s"select to_yaml(cast(1234.50404 as decimal(30,10)), $UseFullScalarType) r").as[String].head

    val yaml = YamlDecoder.yaml

    val dec = BigDecimal(1234.50404).setScale(10).bigDecimal
    val obj = yaml.load[java.math.BigDecimal](str);
    assert(obj == dec)
  }

  @Test
  def sqlTest: Unit = evalCodeGens {
    def serDe(mapStr: String) {
      val df = sparkSession.sql("select array(1,2,3,4,5) og")
        .selectExpr("*", s"to_yaml(og$mapStr) y")
        .selectExpr("*", "from_yaml(y, 'array<int>') f")
        .filter("f == og")
      //df.show
      assert(df.count == 1)
    }

    serDe("")
    serDe(s", $UseFullScalarType")
  }

  @Test
  def nonLiteralMapEntriesTest: Unit = evalCodeGens {
    try {
      sparkSession.sql("select array(1,2,3,4,5) og")
        .selectExpr("*", s"to_yaml(og, map(og, 1)) y")
      fail("Should have thrown as og is not a literal")
    } catch {
      case t: QualityException => assert(t.getMessage.contains("Could not process a literal map with expression"))
    }
    try {
      sparkSession.sql("select array(1,2,3,4,5) og")
        .selectExpr("*", s"to_yaml(og, map(1, 'true')) y")
      fail("Should have thrown as 1 is not a string literal")
    } catch {
      case t: QualityException => assert(t.getMessage.contains("Could not process a literal map with expression"))
    }
    try {
      sparkSession.sql("select array(1,2,3,4,5) og")
        .selectExpr("*", s"to_yaml(og, map('1', og)) y")
      fail("Should have thrown as og is not a literal")
    } catch {
      case t: QualityException => assert(t.getMessage.contains("Could not process a literal map with expression"))
    }
  }

}
