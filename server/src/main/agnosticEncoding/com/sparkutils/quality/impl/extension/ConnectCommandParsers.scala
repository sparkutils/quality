package com.sparkutils.quality.impl.extension

import com.sparkutils.quality.impl.extension.ConnectCommandParsers.{NoneQuoted, nameDFOrNoneS}
import com.sparkutils.quality.impl.util.SerializingShim.combineImpl
import com.sparkutils.shim.AbstractInjectableParser
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, ShimUtils, SparkSession}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

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
      ShimUtils.logicalPlan(combineImpl(rules, lfdf, oedf, pps, qldf, gloedf).get)
    } else
      super.parsePlan(sqlText)
  }
}
