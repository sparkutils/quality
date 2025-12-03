package com.sparkutils.quality.impl

import com.sparkutils.quality.LambdaFunction
import com.sparkutils.quality.impl.qualityFunctions.LambdaFunctions

object QualitySparkUtils {

  def registerLambdaFunctions(functions: Seq[LambdaFunction]): Unit =
    if (functions.nonEmpty)
      LambdaFunctions.registerLambdaFunctions(functions)
    else
      ()
}
