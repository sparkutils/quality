package com.sparkutils.qualityTests

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.{QualitySparkUtils, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}

import java.util.concurrent.atomic.AtomicReference

case class AnalysisException(message: String) extends Exception(message)

object SparkTestUtils {

  /**
   * 9.1.dbr oss hofs cannot work in Quality tests, but on other versions does
   */
  val skipHofs = true

  def testStaticConfigKey(k: String) =
    if (QualitySparkUtils.isStaticConfigKey(k)) {
      throw new AnalysisException(s"Cannot modify the value of a static config: $k")
    }

  protected var tpath = new AtomicReference[String]("./target/testData")

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
    Some(sparkSession.sessionState.catalog.lookupFunction(FunctionIdentifier(name), exps))

  def getCorrectPlan(sparkPlan: SparkPlan): SparkPlan =
    sparkPlan


  def enumToScala[A](enum: java.util.Enumeration[A]) = {
    import scala.collection.JavaConverters._

    enumerationAsScalaIterator(enum)
  }
}
