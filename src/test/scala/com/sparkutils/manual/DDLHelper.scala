package com.sparkutils.manual

import com.sparkutils.quality.impl.util.CombinedRuleSuiteRows
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders

object DDLHelper {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[1]").getOrCreate()
    import sparkSession.implicits._
    println(AgnosticEncoders.agnosticEncoderFor[CombinedRuleSuiteRows].schema.toDDL)
  }
}
