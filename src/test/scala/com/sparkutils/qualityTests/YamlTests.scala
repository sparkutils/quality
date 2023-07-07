package com.sparkutils.qualityTests

import com.sparkutils.quality
import com.sparkutils.quality._
import functions._
import types._
import impl.imports.RuleResultsImports.packId
import com.sparkutils.quality.impl.util.{Arrays, PrintCode}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DataTypes, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Encoder, SaveMode}
import org.junit.Test
import org.scalatest.FunSuite
import java.util.UUID

import com.sparkutils.quality.impl.yaml.{YamlDecoderExpr, YamlEncoderExpr}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal

import scala.language.postfixOps

class YamlTests extends FunSuite with RowTools with TestUtils {

  def doSerDeTest(original: String, ddl: String) = evalCodeGens {
    val df = sparkSession.sql(s"select $original bits")
      .select(col("bits"), new Column(YamlEncoderExpr( col("bits").expr )).as("converted"))
      .select(expr("*"),  new Column(YamlDecoderExpr( col("converted").expr , DataType.fromDDL(ddl))).as("deconverted"))

    val r = df.select(col("bits").as("og"), comparable_maps(col("bits")).as("bits"), col("converted"), col("deconverted").as("og_deconverted"), comparable_maps(col("deconverted")).as("deconverted"))
    val filtered= r.filter("deconverted = bits or deconverted is null and bits is null")
    //r.show
    assert(filtered.count == 1)
  }

  @Test
  def structsAsKeys: Unit =
    doSerDeTest("map(named_struct('col1','group', 'col2','parts'), 1235, named_struct('col1','more','col2','parts'), 2666, named_struct('col1',null,'col2','parts'), null)",
      "map<struct<col1: String, col2: String>, long>")

  @Test
  def sequenceAsKeys: Unit =
    doSerDeTest("map(array('col1','group', 'col2','parts'), 1235, array('col1','more','col2','parts'), 2666, array(null,'more',null,'parts'), 2654645666)",
      "map<array<String>, long>")

  @Test
  def structsAsValues: Unit =
    doSerDeTest("map(1235, named_struct('col1','group', 'col2','parts'), 2666, named_struct('col1','more','col2','parts'), 546456, null)",
      "map<long, struct<col1: String, col2: String>>")

  @Test
  def mapsAsValues: Unit =
    doSerDeTest("map(named_struct('col1','group', 'col2','parts'), map(1235, null, 1234, 466), named_struct('col1','more','col2','parts'), map(1235, null, 1234, null), named_struct('col1',null,'col2','parts'), null)",
      "map<struct<col1: String, col2: String>, map<long,long>>")

  @Test
  def sequencesAsValues: Unit =
    doSerDeTest("map(array('col1','group', 'col2','parts'), array('col1','group', 'col2','parts'), array('col1','more','col2','parts'), array('col1','group', 'col2','parts'), array(null,'more',null,'parts'), null)",
      "map<array<String>, array<String>>")

  @Test
  def sequenceAsKeysDecimals: Unit =
    doSerDeTest("map(array('col1','group', 'col2','parts'), cast( 1235.34452 as decimal(38,17)), array('col1','more','col2','parts'), cast( 2666.2345 as decimal(38,17)))",
      "map<array<String>, decimal(38,17)>")

  @Test
  def theRest: Unit = {

    def doSerDe(original: String, ddl: String) = {
      doSerDeTest(original, ddl)
      doSerDeTest(s"named_struct('value', $original)", s"struct<value : $ddl>")
    }

    doSerDe("null", "string")

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

  }
}
