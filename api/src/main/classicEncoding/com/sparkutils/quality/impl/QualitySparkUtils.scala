package com.sparkutils.quality.impl

import com.sparkutils.quality.LambdaFunction
import org.apache.spark.sql.SparkSession

object QualitySparkUtils {

  def registerLambdaFunctions(functions: Seq[LambdaFunction]): Unit =
    if (functions.nonEmpty)
      SparkSession.active match {
        case s: classic.SparkSession =>
          LambdaFunctions.registerLambdaFunctions(functions)
        case _ =>
          val s = SparkSession.active
          val command = s"$CREATE_FUNCTION_PREFIX\n" +
            functions.map {
              f =>
                // needs to be registered via the extension
                s"${f.name}$WITH_TOKEN${f.rule}"
            }.mkString(DIVIDER)
          s.sql(command)
      }
    else
      ()
}
