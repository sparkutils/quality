package com.sparkutils.qualityTests

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec

import java.io.File
import java.util.concurrent.atomic.AtomicReference

case class AnalysisException(message: String) extends Exception(message)

object SparkTestUtils {

  /**
   * 9.1.dbr oss hofs cannot work in Quality tests, but on other versions does
   */
  val skipHofs = false

  def testStaticConfigKey(k: String) =
    if (SQLConf.isStaticConfigKey(k)) {
      throw new AnalysisException(s"Cannot modify the value of a static config: $k")
    }

  protected var tpath = new AtomicReference[String](new File("./target/testData").getAbsolutePath)

  def ouputDir = tpath.get


  def setPath(newPath: String) = {
    tpath.set(newPath)
    // when this is called set the docs path as well as an offset
    tdocpath.set(newPath + "/docs")
  }

  def path(suffix: String) = s"${tpath.get}/$suffix"

  protected var tdocpath = new AtomicReference[String]("./docs/advanced")
  def docDir = tpath.get
  def docpath(suffix: String) = s"${tdocpath.get}/$suffix"

  def resolveBuiltinOrTempFunction(sparkSession: SparkSession)(name: String, exps: Seq[Expression]): Option[Expression] =
    sparkSession.sessionState.catalog.resolveBuiltinOrTempFunction(name, exps)

  def getCorrectPlan(sparkPlan: SparkPlan): SparkPlan =
    if (sparkPlan.children.isEmpty)
    // assume it's AQE
      sparkPlan match {
        case aq: AdaptiveSparkPlanExec => aq.initialPlan
        case _ => sparkPlan
      }
    else
      sparkPlan
}
