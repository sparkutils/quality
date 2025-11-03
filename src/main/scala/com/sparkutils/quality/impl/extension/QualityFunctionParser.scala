package com.sparkutils.quality.impl.extension

import com.sparkutils.quality.Id
import com.sparkutils.quality.impl.LambdaFunctionImpl
import com.sparkutils.quality.impl.extension.QualityFunctionParser.{CREATE_FUNCTION_PREFIX, WITH_TOKEN}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, NoopCommand}
import org.apache.spark.sql.qualityFunctions.LambdaFunctions
import org.apache.spark.sql.types.{DataType, StructType}

import scala.io.Source

object QualityFunctionParser {
  val CREATE_FUNCTION_PREFIX = "CREATE QUALITY FUNCTION"
  val DIVIDER = " _END_OF_USER_FUNCTION_ "
  val WITH_TOKEN = " _WITH_IMPL_ "
}

case class QualityFunctionParser(sparkSession: SparkSession, delegate: ParserInterface) extends ParserInterface with Logging {

  override def parsePlan(sqlText: String): LogicalPlan =
    if (sqlText.startsWith(CREATE_FUNCTION_PREFIX)) {

      val lines = Source.fromString(sqlText).getLines()
      val functions =
        for{ line <- lines.drop(1) } yield { // first not needed
          val idx = line.indexOf(WITH_TOKEN)
          val name = line.take(idx)
          val rule = line.drop(idx + WITH_TOKEN.length)
          logDebug(s"Quality Rule via extension $name with rule: $rule")
          LambdaFunctionImpl(name, rule, Id(-1,-1))
        }

      val sess = SparkSession.getActiveSession
      try {
        SparkSession.setActiveSession(sparkSession)
        LambdaFunctions.registerLambdaFunctions(functions.toSeq)
        NoopCommand(CREATE_FUNCTION_PREFIX, Seq.empty)
      } finally {
        sess.foreach(SparkSession.setActiveSession)
      }

    } else
      delegate.parsePlan(sqlText)

  override def parseExpression(sqlText: String): Expression = delegate.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier = delegate.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = delegate.parseFunctionIdentifier(sqlText)

  override def parseMultipartIdentifier(sqlText: String): Seq[String] = delegate.parseMultipartIdentifier(sqlText)

  override def parseQuery(sqlText: String): LogicalPlan = delegate.parseQuery(sqlText)

  override def parseRoutineParam(sqlText: String): StructType = delegate.parseRoutineParam(sqlText)

  override def parseTableSchema(sqlText: String): StructType = delegate.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType = delegate.parseDataType(sqlText)
}
