package com.sparkutils.qualityTests.util

import com.sparkutils.quality
import com.sparkutils.quality.impl.{RuleSuiteHelpers, Runners}
import com.sparkutils.quality.impl.extension.FunNRewrite
import com.sparkutils.quality.{RuleSuite, classicFunctions, ruleRunner}
import com.sparkutils.testing.SparkTestUtils.{connectMemory, scoverageClassPathsConfig, useDebugConnectLogs}
import com.sparkutils.testing._
import com.sparkutils.testing.markers.{ConnectSafe, DontRunOnPureConnect}
import com.sparkutils.testing.sessionStrategies.{GlobalSession, SharedSessions}
import org.apache.spark.sql.ClassicQualitySparkUtils.DatasetBase
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Dataset, Row, ShimUtils, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite, TestSuite}

trait ClassicSharedTests extends FunSuite with TestSetup {

  override val currentSessionsHolder: SessionsStateHolder = GlobalSession

  override val runWith: ConnectionType = ClassicOnly

  //override val loggingLevel = "DEBUG"

  /**
   * enable funN rewrites, runs the test twice, once under the optimisation, once without
   */
  def funNRewrites(u: => Unit): Unit = {
    if (!inConnect.get()) {
      testPlan(FunNRewrite)(u)
    } else {
      u
    }
  }
  /**
   * enable funN rewrites for one test run only
   */
  def justfunNRewrite(u: => Unit): Unit = {
    if (!inConnect.get()) {
      testPlan(FunNRewrite, secondRunWithoutPlan = false)(u)
    } else {
      u
    }
  }

}


trait SharedConnectTests extends SharedPureConnectTests with ClassicSharedTests with DontRunOnPureConnect {

}

trait TestSetup extends SparkTestSuite with TestUtilsBase with SharedSessions { self: TestSuite =>

  override def beforeAll(): Unit = {
    // no-op to force it to be created
    forceLoad
    super.beforeAll()

    cleanupOutput()

    withClassicAsActive({
      quality.registerQualityFunctions()
    })
  }

  override def connectServerLoggingLevel = "DEBUG"

  override def sparkConnectServerConfig(): Map[String, String] =
    super.sparkConnectServerConfig() + //useDebugConnectLogs +
      scoverageClassPathsConfig + connectMemory("4g") +
      (("spark.sql.extensions", "com.sparkutils.quality.impl.extension.QualitySparkExtension")) + // text used for connect only tests in dbr
      (("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=connect_metastore_db;create=true")) +
      (("spark.sql.codegen.factoryMode", "NO_CODEGEN")) /*+
      (("spark.sql.queryExecutionListeners", "com.sparkutils.quality.impl.extension.EchoListener"))*/

}

trait SharedPureConnectTests extends FunSuite with TestSetup with ConnectSafe {

  override val currentSessionsHolder: SessionsStateHolder = GlobalSession

  override val runWith: ConnectionType = UseBoth

}

trait TestUtilsBase extends SparkTestSuite {

  /**
   * Adds a DataQuality field using the RuleSuite and RuleSuiteResult structure
   * @param dataFrame
   * @param rules
   * @param name
   * @return
   */
  def taddDataQuality(dataFrame: Dataset[Row], rules: RuleSuite, name: String = "DataQuality", compileEvals: Boolean = true): Dataset[Row] = {
    import org.apache.spark.sql.functions.expr
    val tdf = dataFrame.drop(name) // some gen tests add this
    val rr =
      if (ShimUtils.isClassic(SparkSession.active))
        Runners.ruleRunner(rules, compileEvals, resolveWith = if (doResolve.get()) Some(tdf) else None, forceRunnerEval = false).get
      else
        ShimUtils.callFunction("dq_rule_runner", lit(RuleSuiteHelpers.serialize(rules)))

    tdf.select(expr("*"), rr.as(name))
  }

  /**
   * Adds a DataQuality field using the RuleSuite and RuleSuiteResult structure for use with dataset.transform functions
   * @param rules
   * @param name
   * @return
   */
  def taddDataQualityF[P[R] >: DatasetBase[R]](rules: RuleSuite, name: String = "DataQuality"): P[Row] => P[Row] =
    (p: P[Row]) => taddDataQuality(p.asInstanceOf[Dataset[Row]], rules, name)

  /**
   * Adds two columns, one for overallResult and the other the details, allowing 30-50% performance gains for simple filters
   * @param dataFrame
   * @param rules
   * @param overallResult
   * @param resultDetails
   * @return
   */
  def taddOverallResultsAndDetails(dataFrame: Dataset[Row], rules: RuleSuite, overallResult: String = "DQ_overallResult",
                                  resultDetails: String = "DQ_Details"): Dataset[Row] = {
    val temporaryDQname: String = "DQ_TEMP_Quality"
    taddDataQuality(dataFrame, rules, temporaryDQname).
      selectExpr("*",s"$temporaryDQname.overallResult as $overallResult",
        s"ruleSuiteResultDetails($temporaryDQname) as $resultDetails").drop(temporaryDQname)
  }

  /**
   * Adds two columns, one for overallResult and the other the details, allowing 30-50% performance gains for simple filters, for use in dataset.transform functions
   * @param rules
   * @param overallResult
   * @param resultDetails
   * @return
   */
  def taddOverallResultsAndDetailsF[P[R] >: DatasetBase[R]](rules: RuleSuite, overallResult: String = "DQ_overallResult",
                                   resultDetails: String = "DQ_Details"): P[Row] => P[Row] =
    (p: P[Row]) => taddOverallResultsAndDetails(p.asInstanceOf[Dataset[Row]], rules, overallResult, resultDetails)

  def loadsOf(thunk: => Unit, runs: Int = 3000): Unit = {
    var passed = 0
    for{ i <- 0 until runs }{
      try {
        thunk
        passed += 1
      } catch {
        case e: org.scalatest.exceptions.TestFailedException => println("failed "+e.getMessage())
        case t: Throwable => println("failed unexpectedly "+t.getMessage())
      }
    }
    assert(passed == runs, "Should have passed all of them, nothing has changed in between runs")
  }

}
