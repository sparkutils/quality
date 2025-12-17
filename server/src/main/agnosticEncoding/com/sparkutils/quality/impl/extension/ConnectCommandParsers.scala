package com.sparkutils.quality.impl.extension

import com.sparkutils.quality.impl.extension.ConnectCommandParsers.{NoneQuoted, nameDFOrNoneS}
import com.sparkutils.quality.impl.extension.QualityVersionedRulesConstants.{FROM_DF, QUALITY_VERSIONED, QUALITY_VERSIONED_LAMBDAS_FROM_DF, QUALITY_VERSIONED_OUTPUT_EXPRESSIONS_FROM_DF, QUALITY_VERSIONED_RULES_FROM_DF}
import com.sparkutils.quality.impl.util.SerializingShim.combineImplI
import com.sparkutils.quality.impl.util.SimpleVersioning
import com.sparkutils.shim.AbstractInjectableParser
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, ShimUtils, SparkSession}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType

object ConnectCommandParsers {
  val combine = "QUALITY COMBINE RULESUITES"
  val NoneQuoted = "`None`"

  def nameDFOrNoneS(cmdPart: String, sparkSession: SparkSession): Option[DataFrame] =
    cmdPart match {
      case NoneQuoted => None
      case view => Some(sparkSession.sql(s"select * from global_temp.$view"))
    }
}

case class ConnectCommandParsers(sparkSession: SparkSession, delegate: ParserInterface) extends AbstractInjectableParser(sparkSession, delegate) with Logging {

  override def parsePlan(sqlText: String): LogicalPlan = {
    if (sqlText.startsWith(ConnectCommandParsers.combine)) {
      // sql(s"QUALITY COMBINE RULESUITES $rname, $lfname, $oename, ${probablePass.map(_.toString).getOrElse("`None`")}, $glname, $gloename"))
      val nameDFOrNone = nameDFOrNoneS(_, sparkSession)
      // TODO full greedy parsers or just let spark throw errors from usage?
      val cmd = sqlText.drop(ConnectCommandParsers.combine.length).split(',').map(_.trim).toIndexedSeq

      val rules = sparkSession.sql(s"select * from global_temp.${cmd(0)}")
      val lfdf = nameDFOrNone(cmd(1))
      val oedf = nameDFOrNone(cmd(2))
      val pps = cmd(3) match {
        case NoneQuoted => None
        case d => d.toDoubleOption
      }
      val qldf = nameDFOrNone(cmd(4))
      val gloedf = nameDFOrNone(cmd(5))
      ShimUtils.logicalPlan(combineImplI(rules, lfdf, oedf, pps, qldf, gloedf))
    } else
      if (sqlText.startsWith(QUALITY_VERSIONED)) {
        val fromOffset = sqlText.indexOf(FROM_DF) + FROM_DF.length
        val viewName = sqlText.substring(fromOffset).replace(';',' ')
        val df = sparkSession.sql(s"select * from global_temp.$viewName")
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
          }
        )
      } else
        super.parsePlan(sqlText)
  }
}
