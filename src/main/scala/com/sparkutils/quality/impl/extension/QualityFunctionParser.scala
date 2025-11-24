package com.sparkutils.quality.impl.extension

import com.sparkutils.quality.Id
import com.sparkutils.quality.impl.LambdaFunctionImpl
import com.sparkutils.quality.impl.extension.QualityFunctionParser.{CREATE_FUNCTION_PREFIX, DIVIDER, WITH_TOKEN}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, NoopCommand}
import org.apache.spark.sql.qualityFunctions.LambdaFunctions
import com.sparkutils.shim.AbstractInjectableParser

object QualityFunctionParser {
  val CREATE_FUNCTION_PREFIX = "CREATE QUALITY FUNCTION "
  val DIVIDER = " _END_OF_USER_FUNCTION_ "
  val WITH_TOKEN = " _WITH_IMPL_ "
}

case class QualityFunctionParser(sparkSession: SparkSession, delegate: ParserInterface) extends AbstractInjectableParser(sparkSession, delegate) with Logging {

  override def parsePlan(sqlText: String): LogicalPlan =
    if (sqlText.startsWith(CREATE_FUNCTION_PREFIX)) {
      val full = sqlText.drop(CREATE_FUNCTION_PREFIX.length)
      if (full.trim.isEmpty) {
        NoopCommand(CREATE_FUNCTION_PREFIX, scala.Seq.empty)
      } else {
        val functions =
          full.split(DIVIDER).map { line =>
            val idx = line.indexOf(WITH_TOKEN)
            val name = line.take(idx).trim
            val rule = line.drop(idx + WITH_TOKEN.length).trim
            logDebug(s"Quality Rule via extension $name with rule: {$rule} - rule end")
            LambdaFunctionImpl(name, rule, Id(-1, -1))
          }

        val sess = SparkSession.getActiveSession
        try {
          SparkSession.setActiveSession(sparkSession)
          LambdaFunctions.registerLambdaFunctions(functions.toSeq)
          NoopCommand(CREATE_FUNCTION_PREFIX, scala.Seq.empty)
        } finally {
          sess.foreach(SparkSession.setActiveSession)
        }
      }

    } else
      delegate.parsePlan(sqlText)

}
