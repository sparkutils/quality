package com.sparkutils.qualityTests

import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import com.sparkutils.quality
import com.sparkutils.quality.{RuleSuite, ruleRunner}
import com.sparkutils.qualityTests.SparkTestUtils.getCorrectPlan
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{CodegenObjectFactoryMode, Expression}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.junit.Before

import java.io.File

trait TestUtils {
  val hostMode = {
    val tmp = System.getenv("QUALITY_SPARK_HOSTS")
    if (tmp eq null)
      "*"
    else
      tmp
  }

  def sparkSessionF: SparkSession = {
    val sparkSession = registerFS(SparkSession.builder()).config("spark.master", s"local[$hostMode]").config("spark.ui.enabled", false).getOrCreate()
    if (excludeFilters) {
      sparkSession.conf.set("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.InferFiltersFromGenerate")
    }

    sparkSession.conf.set("spark.sql.optimizer.nestedSchemaPruning.enabled", true)
    // only a visual change
    // sparkSession.conf.set("spark.sql.legacy.castComplexTypesToString.enabled", true)
    sparkSession.sparkContext.setLogLevel("ERROR") // set to debug to get actual code lines etc.
    sparkSession
  }

  def sqlContextF = sparkSessionF.sqlContext

  val excludeFilters = {
    val tmp = System.getProperty("excludeFilters")
    if (tmp eq null)
      true
    else
      tmp.toBoolean
  }

  /**
   * Allows bare naked to be used instead of winutils for testing / dev
   */
  def registerFS(sparkSessionBuilder: SparkSession.Builder): SparkSession.Builder =
    if (System.getProperty("os.name").startsWith("Windows"))
      sparkSessionBuilder.config("spark.hadoop.fs.file.impl",classOf[BareLocalFileSystem].getName)
    else
      sparkSessionBuilder

  val sparkSession = sparkSessionF
  val sqlContext = sqlContextF

  val outputDir = SparkTestUtils.ouputDir

  def stop(start: Long) = {
    val stop = System.currentTimeMillis()
    if (stop - start > 60000)
      ((stop - start) / 1000 / 60, "m")
    else
      (stop - start, "ms")
  }

  {
    sys.props.put("spark.testing","yes yes it is")
  }

  def cleanUp(target: String): Unit = {
    import scala.reflect.io.Directory
    val outdir = new Directory(new java.io.File(target))
    outdir.deleteRecursively()
  }

  def cleanupOutput(): Unit =
    cleanUp(outputDir)

  @Before
  def setup(): Unit = {
    cleanupOutput()
    quality.registerQualityFunctions()
  }

  // if this blows then debug on CodeGenerator 1294, 1299 and grab code.body
  def forceCodeGen[T](f: => T): T = {
    val codegenMode = CodegenObjectFactoryMode.CODEGEN_ONLY.toString

    withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegenMode) {
      f
    }
  }

  def forceInterpreted[T](f: => T): T = {
    val codegenMode = CodegenObjectFactoryMode.NO_CODEGEN.toString

    withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegenMode) {
      f
    }
  }

  /**
   * Forces resolveWith to be used where possible
   */
  val doResolve =
    new ThreadLocal[Boolean] {
      override def initialValue(): Boolean = false
    }

  def doWithResolve[T](f: => T): T =
    try {
      doResolve.set(true)
      f
    } finally {
      doResolve.set(false)
    }

  /**
   * runs the same test with both eval and codegen, then does the same again using resolveWith
   * @param f
   * @tparam T
   * @return
   */
  def evalCodeGens[T](f: => T):(T,T,T,T)  =
    (forceCodeGen(f), forceInterpreted(f), forceCodeGen(doWithResolve(f)), forceInterpreted(doWithResolve(f)))

  /**
   * runs the same test with both eval and codegen
   * @param f
   * @tparam T
   * @return
   */
  def evalCodeGensNoResolve[T](f: => T):(T,T)  =
    (forceCodeGen(f), forceInterpreted(f))

  /**
   * Sets all SQL configurations specified in `pairs`, calls `f`, and then restores all SQL
   * configurations.
   */
  protected def withSQLConf[T](pairs: (String, String)*)(f: => T): T = {
    val conf = SQLConf.get
    val (keys, values) = pairs.unzip
    val currentValues = keys.map { key =>
      if (conf.contains(key)) {
        Some(conf.getConfString(key))
      } else {
        None
      }
    }
    (keys, values).zipped.foreach { (k, v) =>
      SparkTestUtils.testStaticConfigKey(k)
      conf.setConfString(k, v)
    }
    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => conf.setConfString(key, value)
        case (key, None) => conf.unsetConf(key)
      }
    }
  }


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
    val rr = ruleRunner(rules, compileEvals, resolveWith = if (doResolve.get()) Some(tdf) else None)
    tdf.select(expr("*"), rr.as(name))
  }

  /**
   * Adds a DataQuality field using the RuleSuite and RuleSuiteResult structure for use with dataset.transform functions
   * @param rules
   * @param name
   * @return
   */
  def taddDataQualityF(rules: RuleSuite, name: String = "DataQuality"): Dataset[Row] => Dataset[Row] =
    taddDataQuality(_, rules, name)

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
  def taddOverallResultsAndDetailsF(rules: RuleSuite, overallResult: String = "DQ_overallResult",
                                   resultDetails: String = "DQ_Details"): Dataset[Row] => Dataset[Row] =
    taddOverallResultsAndDetails(_, rules, overallResult, resultDetails)

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

  lazy val sparkFullVersion = {
    val pos = classOf[Expression].getPackage.getSpecificationVersion
    if (pos eq null) // DBR is always null
      SparkSession.active.version
    else
      pos
  }

  lazy val sparkVersion = {
    sparkFullVersion.split('.').take(2).mkString(".")
  }

  /**
   * Don't run this test on 2.4 - typically due to not being able to control code gen properly
   */
  def not2_4(thunk: => Unit) =
    if (sparkVersion != "2.4") thunk

  /**
   * Don't run this test on 3.4 - gc's on code gen
   */
  def not3_4(thunk: => Unit) =
    if (sparkVersion != "3.4") thunk

  /**
   * Don't run this test on 3.4 or greater - gc's on code gen
   */
  def not3_4_or_above(thunk: => Unit) =
    if (sparkVersion.replace(".","").toInt < 34) thunk

  /**
   * Scalar subqueries etc. only work on 3.4 and above
   * @param thunk
   */
  def v3_4_and_above(thunk: => Unit) =
    if (sparkVersion.replace(".","").toInt >= 34) thunk

  /**
   * INTERVAL MONTH etc. not supported below 3.2
   * @param thunk
   */
  def v3_2_and_above(thunk: => Unit) =
    if (sparkVersion.replace(".","").toInt >= 32) thunk

  /**
   * Only run this on 2.4
   * @param thunk
   */
  def only2_4(thunk: => Unit) =
    if (sparkVersion == "2.4") thunk

  /**
   * transform_values and transform_keys pattern match on list only which doesn't work with seq in the _lambda_ param re-writes
   * @param thunk
   */
  def not2_4_or_3_0_or_3_1(thunk: => Unit) =
    if (!Set("2.4", "3.0", "3.1").contains(sparkVersion)) thunk

  lazy val onDatabricks = {
    val dbfs = new File("/dbfs")
    dbfs.exists
  }

  /**
   * Don't run this test on Databricks - due to either running in a cluster or, in the case of trying for force soe's etc
   * because Databricks defaults and Codegen are different
   */
  def not_Databricks(thunk: => Unit) =
    if (!onDatabricks) thunk

  /**
   * Only run when the extension is enabled
   */
  def onlyWithExtension(thunk: => Unit) = {
    val extensions = sparkSession.sparkContext.getConf.get("spark.sql.extensions","")
    if (extensions.indexOf("com.sparkutils.quality.impl.extension.QualitySparkExtension") > -1) {
      thunk
    }
  }

  /**
   * Checks for an exception, then it's cause(s) for f being true
   * @param t
   * @param f
   * @return
   */
  def anyCauseHas(t: Throwable, f: Throwable => Boolean): Boolean =
    if (f(t))
      true
    else
      if (t.getCause ne null)
        anyCauseHas(t.getCause, f)
      else
        false

  /**
   * Gets pushdowns from a dataset
   * @param sparkPlan
   * @return
   */
  def getPushDowns[T](dataset: Dataset[T]): Seq[Filter] =
    getPushDowns(dataset.queryExecution.executedPlan)

  /**
   * Gets pushdowns from a FileSourceScanExec from a plan
   * @param sparkPlan
   * @return
   */
  def getPushDowns(sparkPlan: SparkPlan): Seq[Filter] =
    getCorrectPlan(sparkPlan).collect {
      case fs: FileSourceScanExec =>
        import scala.reflect.runtime.{universe => ru}

        val runtimeMirror = ru.runtimeMirror(getClass.getClassLoader)
        val instanceMirror = runtimeMirror.reflect(fs)
        val getter = ru.typeOf[FileSourceScanExec].member(ru.TermName("pushedDownFilters")).asTerm.getter
        val m = instanceMirror.reflectMethod(getter.asMethod)
        val res = m.apply(fs).asInstanceOf[Seq[Filter]]

        res
    }.flatten

}
