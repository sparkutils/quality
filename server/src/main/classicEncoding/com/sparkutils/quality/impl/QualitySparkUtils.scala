package com.sparkutils.quality.impl

import org.apache.spark.sql.qualityFunctions.LambdaFunctions

object QualitySparkUtils {

  def registerLambdaFunctions(functions: Seq[LambdaFunction]): Unit =
    LambdaFunctions.registerLambdaFunctions(functions)

}
