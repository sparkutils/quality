package com.sparkutils.quality.impl.extension

import com.sparkutils.quality.QualityException
import com.sparkutils.quality.impl.extension.ConnectCommandParsers.{nameDFOrNoneS, tempView}
import com.sparkutils.quality.impl.extension.QualityCombineConstants.{NoneQuoted, QUALITY_COMBINE}
import com.sparkutils.quality.impl.extension.QualityVersionedRulesConstants.{FROM_DF, QUALITY_VERSIONED, QUALITY_VERSIONED_LAMBDAS_FROM_DF, QUALITY_VERSIONED_OUTPUT_EXPRESSIONS_FROM_DF, QUALITY_VERSIONED_RULESUITES_FROM_DF, QUALITY_VERSIONED_RULES_FROM_DF}
import com.sparkutils.quality.impl.util.SerializingShim.combineImplI
import com.sparkutils.quality.impl.util.SimpleVersioning
import com.sparkutils.shim.AbstractInjectableParser
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, ShimUtils, SparkSession}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, View}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.command.CreateViewCommand
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.util.QueryExecutionListener
/*
class EchoListener extends QueryExecutionListener {

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    if (funcName == "command") {
      qe.logical match {
        case cv: CreateViewCommand =>
          println(s"got $cv  ${cv.plan.getClass.getName}")
          println(s"got ${qe.sparkSession.sessionState.catalog.getTempView(cv.name.table)}");
      }
    }
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {

  }
}
*/
object ConnectCommandParsers {
  def tempView(sparkSession: SparkSession, name: String): DataFrame =
    ShimUtils.ofRows(sparkSession.asInstanceOf[org.apache.spark.sql.classic.SparkSession],
      sparkSession.sessionState.catalog.getTempView(name).getOrElse(throw QualityException(s"Cannot find temp view $name")))

  def nameDFOrNoneS(cmdPart: String, sparkSession: SparkSession): Option[DataFrame] =
    cmdPart match {
      case NoneQuoted => None
      case _ => Some(tempView(sparkSession, cmdPart))
    }
}

case class ConnectCommandParsers(sparkSession: SparkSession, delegate: ParserInterface) extends AbstractInjectableParser(sparkSession, delegate) with Logging {

  override def parsePlan(sqlText: String): LogicalPlan = {
    if (sqlText.startsWith(QUALITY_COMBINE)) {
      // sql(s"QUALITY COMBINE RULESUITES $rname, $lfname, $oename, $glname, $gloename, $rsname"))
      val nameDFOrNone = nameDFOrNoneS(_, sparkSession)
      // TODO full greedy parsers or just let spark throw errors from usage?
      val cmd = sqlText.drop(QUALITY_COMBINE.length).split(',').map(_.trim).toIndexedSeq

      val rules = tempView(sparkSession, cmd(0))

      val lfdf = nameDFOrNone(cmd(1))
      val oedf = nameDFOrNone(cmd(2))
      val qldf = nameDFOrNone(cmd(3))
      val gloedf = nameDFOrNone(cmd(4))
      val rsdf = nameDFOrNone(cmd(5))
      ShimUtils.logicalPlan(combineImplI(rules, lfdf, oedf, qldf, gloedf, rsdf))
    } else
      if (sqlText.startsWith(QUALITY_VERSIONED)) {
        val fromOffset = sqlText.indexOf(FROM_DF) + FROM_DF.length
        val viewName = sqlText.substring(fromOffset).replace(';',' ')
        val df = tempView(sparkSession, viewName)
        val cmd = sqlText.substring(0,fromOffset)
        ShimUtils.logicalPlan(
          cmd match {
            case QUALITY_VERSIONED_RULES_FROM_DF =>
              SimpleVersioning.readVersionedRuleRowsFromDF(df, col("ruleSuiteId").cast(IntegerType),
                col("ruleSuiteVersion").cast(IntegerType),  col("ruleSetId").cast(IntegerType),
                col("ruleSetVersion").cast(IntegerType),  col("ruleId").cast(IntegerType),
                col("ruleVersion").cast(IntegerType),  col("ruleExpr"),  col("ruleEngineSalience").cast(IntegerType),
                col("ruleEngineId").cast(IntegerType),  col("ruleEngineVersion").cast(IntegerType))
            case QUALITY_VERSIONED_LAMBDAS_FROM_DF =>
              SimpleVersioning.readVersionedLambdaRowsFromDF(df, col("name"), col("ruleExpr"),
                col("functionId").cast(IntegerType), col("functionVersion").cast(IntegerType),
                col("ruleSuiteId").cast(IntegerType), col("ruleSuiteVersion").cast(IntegerType))
            case QUALITY_VERSIONED_OUTPUT_EXPRESSIONS_FROM_DF =>
              SimpleVersioning.readVersionedOutputExpressionRowsFromDF(df, col("ruleExpr"),
                col("functionId").cast(IntegerType), col("functionVersion").cast(IntegerType),
                col("ruleSuiteId").cast(IntegerType), col("ruleSuiteVersion").cast(IntegerType))
            case QUALITY_VERSIONED_RULESUITES_FROM_DF =>
              SimpleVersioning.readVersionedRuleSuitesFromDF(df,
                col("ruleSuiteId").cast(IntegerType), col("ruleSuiteVersion").cast(IntegerType),
                col("functionId").cast(IntegerType), col("functionVersion").cast(IntegerType),
                col("probablePass").cast(DoubleType)
              )
          }
        )
      } else
        super.parsePlan(sqlText)
  }
}
