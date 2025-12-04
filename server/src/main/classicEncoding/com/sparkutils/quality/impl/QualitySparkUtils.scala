package com.sparkutils.quality.impl

import com.sparkutils.quality
import com.sparkutils.quality.impl.LambdaFunctionImpl.LambdaFunctionOps
import org.apache.spark.sql.qualityFunctions.LambdaFunctions

object QualitySparkUtils {

  def registerLambdaFunctions(functions: Seq[quality.LambdaFunction]): Unit =
    LambdaFunctions.registerLambdaFunctions(functions.map(_.parsed))

}
