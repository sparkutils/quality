package com.sparkutils.qualityTests.util

import com.sparkutils.testing.{ClassicOnly, ConnectionType, Sessions}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

//case class Test(a: Int, b: Int)

object ResultHelper extends RowTools {


  override val connectionType: ConnectionType = ClassicOnly

  override def sessions: Sessions = createSparkSessions(connectionType)

  def longColsWithDQ(max: Int, dataType: DataType) =
    (for( c <- 0 until max)
      yield StructField(c.toString, LongType)) :+ StructField("DataQuality", dataType)

  def longSchema(maxCols: Int, dataType: DataType = StringType) = StructType(longColsWithDQ(maxCols, dataType))

  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession.builder().config("spark.master", "local").getOrCreate()
    val sqlContext = sparkSession.sqlContext
/*
    val ds = sqlContext.createDataFrame(Seq.empty[Test])
    ds.show */
  }
}
