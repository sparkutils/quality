package com.sparkutils.quality.impl

import org.apache.spark.sql.qualityFunctions.LambdaFunctions
import org.apache.spark.sql.SparkSession

object QualitySparkUtils {

  def registerLambdaFunctions(functions: Seq[LambdaFunction]): Unit =
    LambdaFunctions.registerLambdaFunctions(functions)

}
