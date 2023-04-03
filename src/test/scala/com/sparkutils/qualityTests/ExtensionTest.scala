package com.sparkutils.qualityTests

import java.util.UUID

import com.sparkutils.quality.impl.extension.QualitySparkExtension
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.junit.{Before, Test}
import org.scalatest.FunSuite

// including rowtools so standalone tests behave as if all of them are running and for verify compatibility
class ExtensionTest extends FunSuite with RowTools with TestUtils {

  @Before
  override def setup(): Unit = {
    // nothing from TestUtils is needed
  }

  @Test
  def testExtension(): Unit = not2_4 { not_Databricks { // will never work on 2.4 and Databricks has a fixed session
    var tsparkSession: SparkSession = null
    var sqlContext: SQLContext = null

    try {
      try{
        sparkSessionF.close() // needed to stop the below being ignored
      } catch {
        case t: Throwable => fail( "Could not shut down the wrapping spark",t)
      }
      // attempt to create a new session
      tsparkSession = SparkSession.builder().config("spark.master", s"local[$hostMode]").config("spark.ui.enabled", false).
        config("spark.sql.extensions", classOf[QualitySparkExtension].getName())
        .getOrCreate()
      sqlContext = tsparkSession.sqlContext
      tsparkSession.sparkContext.setLogLevel("ERROR")

      val stable = sqlContext
      import stable.implicits._

      val orig = UUID.randomUUID()
      val uuid = orig.toString

      val res = sqlContext.sql(s"select longPairFromUUID('$uuid') as fparts").selectExpr( "as_uuid(fparts.lower, fparts.higher) as asUUIDExpr")
      val sres = res.as[String].head()
      assert(sres == uuid)

    } finally {
      try {
        if (tsparkSession ne null) {
          tsparkSession.close()
        }
      } finally {}
    }
  }}
}
