package com.sparkutils.quality.impl.qualityFunctions

import com.sparkutils.quality.LambdaFunction
import com.sparkutils.quality.impl.LambdaFunctionImpl.LambdaFunctionOps
import com.sparkutils.testing.ConnectWhenForced.someOrForcedConnect
import org.apache.spark.sql.{ShimUtils, SparkSession}

object LambdaFunctions {
  /**
   * Returns false when in a connect SparkSession and otherwise obeys the testing forced connect logic, registering directly
   * with the current classic spark session as needed by the test / calling code.
   * @param functions
   * @return
   */
  def registerLambdaFunctions(functions: Seq[LambdaFunction]): Boolean = 
    if (ShimUtils.isClassic(SparkSession.active))
      someOrForcedConnect(
        org.apache.spark.sql.qualityFunctions.LambdaFunctions.registerLambdaFunctions(
          functions.map(_.parsed)
        )
      ).isDefined
    else
      false
}
