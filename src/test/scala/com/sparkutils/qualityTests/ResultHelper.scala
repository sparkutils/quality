package com.sparkutils.qualityTests

import org.apache.spark.sql.types.{DataType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.SparkSession

//case class Test(a: Int, b: Int)

object ResultHelper extends RowTools {
  def longColsWithDQ(max: Int, dataType: DataType) =
    (for( c <- 0 until max)
      yield StructField(c.toString, LongType)) :+ StructField("DataQuality", dataType)

  def longSchema(maxCols: Int, dataType: DataType = StringType) = StructType(longColsWithDQ(maxCols, dataType))

  def main(args: Array[String]): Unit = {
    import frameless._

    import scala.collection.JavaConverters._

    import org.apache.spark.sql.functions.{col, expr}

    val sparkSession: SparkSession = SparkSession.builder().config("spark.master", "local").getOrCreate()
    val sqlContext = sparkSession.sqlContext

    import sqlContext.implicits._
/*
    val ds = sqlContext.createDataFrame(Seq.empty[Test])
    ds.show */
  }
}
