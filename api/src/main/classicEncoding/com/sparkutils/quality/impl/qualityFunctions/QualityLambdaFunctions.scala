package com.sparkutils.quality.impl.qualityFunctions

import com.sparkutils.quality.LambdaFunction
import com.sparkutils.quality.impl.extension.QualityFunctionParserConstants.{CREATE_FUNCTION_PREFIX, DIVIDER, WITH_TOKEN}
import com.sparkutils.quality.impl.qualityFunctions.LambdaFunctions
import org.apache.spark.sql.{ShimUtils, SparkSession}

object QualityLambdaFunctions {

  def registerLambdaFunctions(functions: Seq[LambdaFunction]): Unit =
    LambdaFunctions.registerLambdaFunctions(functions)

}
