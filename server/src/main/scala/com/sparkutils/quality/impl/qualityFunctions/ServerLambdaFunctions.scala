package com.sparkutils.quality.impl.qualityFunctions

import com.sparkutils.quality.LambdaFunction
import com.sparkutils.quality.impl.LambdaFunctionImpl.LambdaFunctionOps

object LambdaFunctions {
  def registerLambdaFunctions(functions: Seq[LambdaFunction]): Unit =
    org.apache.spark.sql.qualityFunctions.LambdaFunctions.registerLambdaFunctions(
      functions.map(_.parsed)
    )
}
