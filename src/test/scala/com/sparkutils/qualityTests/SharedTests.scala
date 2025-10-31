package com.sparkutils.qualityTests

import com.sparkutils.quality.impl.extension.FunNRewrite
import com.sparkutils.quality.{RuleSuite, ruleRunner}
import com.sparkutils.testing.{ClassicOnly, ConnectionType}
import com.sparkutils.testing.sessionStrategies.{GlobalSession, SharedSessions}
import org.apache.spark.sql.QualitySparkUtils.DatasetBase
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

trait SharedTests extends FunSuite with TestUtilsBase with SharedSessions with BeforeAndAfterAll {

  override val currentSessionsHolder = GlobalSession

  override def beforeAll(): Unit = {
    super.beforeAll()
    // no-op to force it to be created
    sparkSession.conf
    cleanupOutput()
    com.sparkutils.quality.registerQualityFunctions()
  }
}

trait TestUtilsBase extends com.sparkutils.testing.TestUtils {

  override def connectionType: ConnectionType = ClassicOnly

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
    val rr = ruleRunner(rules, compileEvals, resolveWith = if (doResolve.get()) Some(tdf) else None, forceRunnerEval = false)
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

  /**
   * enable funN rewrites, runs the test twice, once under the optimisation, once without
   */
  lazy val funNRewrites = testPlan(FunNRewrite) _
  /**
   * enable funN rewrites for one test run only
   */
  lazy val justfunNRewrite = testPlan(FunNRewrite, secondRunWithoutPlan = false) _

}
