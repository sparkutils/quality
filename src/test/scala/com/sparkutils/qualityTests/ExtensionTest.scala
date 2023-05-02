package com.sparkutils.qualityTests

import com.sparkutils.quality

import java.util.UUID
import com.sparkutils.quality.impl.extension.{AsUUIDFilter, QualitySparkExtension}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.junit.{Before, Test}
import org.scalatest.FunSuite

// including rowtools so standalone tests behave as if all of them are running and for verify compatibility
class ExtensionTest extends FunSuite with RowTools with TestUtils {

  @Before
  override def setup(): Unit = {
    cleanupOutput()
  }

  def wrapWithExtension(thunk: SparkSession => Unit): Unit = {
    var tsparkSession: SparkSession = null

    try {
      try {
        sparkSessionF.close() // needed to stop the below being ignored
      } catch {
        case t: Throwable => fail("Could not shut down the wrapping spark", t)
      }
      // attempt to create a new session
      tsparkSession = SparkSession.builder().config("spark.master", s"local[$hostMode]").config("spark.ui.enabled", false).
        config("spark.sql.extensions", classOf[QualitySparkExtension].getName())
        .getOrCreate()
      tsparkSession.sparkContext.setLogLevel("ERROR")

      thunk(tsparkSession)

    } finally {
      try {
        if (tsparkSession ne null) {
          tsparkSession.close()
        }
      } finally {}
    }

  }


  @Test
  def testExtension(): Unit = not2_4 { not_Databricks { // will never work on 2.4 and Databricks has a fixed session
    wrapWithExtension{ tsparkSession =>
      import tsparkSession.implicits._

      val orig = UUID.randomUUID()
      val uuid = orig.toString

      val res = tsparkSession.sql(s"select longPairFromUUID('$uuid') as fparts").selectExpr( "as_uuid(fparts.lower, fparts.higher) as asUUIDExpr")
      val sres = res.as[String].head()
      assert(sres == uuid)
    }
  }}

  /*  og is:
  == Parsed Logical Plan ==
  'Filter ('context = 123e4567-e89b-12d3-a456-426614174006)
  +- Project [lower#9L, higher#10L, asuuid(lower#9L, higher#10L) AS context#13]
     +- Relation[lower#9L,higher#10L] parquet

  == Analyzed Logical Plan ==
  lower: bigint, higher: bigint, context: string
  Filter (context#13 = 123e4567-e89b-12d3-a456-426614174006)
  +- Project [lower#9L, higher#10L, asuuid(lower#9L, higher#10L) AS context#13]
     +- Relation[lower#9L,higher#10L] parquet

  == Optimized Logical Plan ==
  Project [lower#9L, higher#10L, asuuid(lower#9L, higher#10L) AS context#13]
  +- Filter (asuuid(lower#9L, higher#10L) = 123e4567-e89b-12d3-a456-426614174006)
     +- Relation[lower#9L,higher#10L] parquet

  == Physical Plan ==
  *(1) Project [lower#9L, higher#10L, asuuid(lower#9L, higher#10L) AS context#13]
  +- *(1) Filter (asuuid(lower#9L, higher#10L) = 123e4567-e89b-12d3-a456-426614174006)
     +- *(1) ColumnarToRow
        +- FileScan parquet [lower#9L,higher#10L] Batched: true, DataFilters: [(asuuid(lower#9L, higher#10L) = 123e4567-e89b-12d3-a456-426614174006)], Format: Parquet, Location: InMemoryFileIndex[file:/C:/Dev/git/quality/target/testData/asymfilter], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<lower:bigint,higher:bigint>

  +--------------------+-------------------+--------------------+
  |               lower|             higher|             context|
  +--------------------+-------------------+--------------------+
  |-6605018797301088250|1314564453825188563|123e4567-e89b-12d...|
  +--------------------+-------------------+--------------------+
   */

  @Test
  def testAsymmetricFilterPlan(): Unit = not2_4 { not_Databricks { // will never work on 2.4 and Databricks has a fixed session
    wrapWithExtension { tsparkSession =>
      import tsparkSession.implicits._

      val theuuid = "123e4567-e89b-12d3-a456-42661417400"

      val therows = for (i <- 0 until 10) yield {
        val uuid = theuuid + i
        val uuidobj = java.util.UUID.fromString(uuid)
        val lower = uuidobj.getLeastSignificantBits
        val higher = uuidobj.getMostSignificantBits
        TestPair(lower, higher)
      }

      // if this is not read from file a LocalRelation will be used and there is no Filter to be pushed down
      therows.toDS.write.parquet(outputDir + "/asymfilter")

      val reread = tsparkSession.read.parquet(outputDir + "/asymfilter")
      val withcontext = reread.selectExpr("*", "as_uuid(lower, higher) as context")

      val ds = withcontext.filter(s"'${theuuid + 6}' = context")

      val pushdowns = ds.queryExecution.executedPlan.collect {
        case fs: FileSourceScanExec => fs.metadata.find(pair => pair._1 == "PushedFilters")
      }.flatten
      if (pushdowns.isEmpty) {
        ds.explain(true)
      }
      assert(pushdowns.size == 1)
      val pusheddown = pushdowns.head._2
      println(s"pushed down $pusheddown")
      assert(pusheddown != "[]", "The predicates were not pushed down")

      val uu = java.util.UUID.fromString(theuuid + "6")
      assert(pusheddown.indexOf(uu.getLeastSignificantBits.toString) > -1)
      assert(pusheddown.indexOf(uu.getMostSignificantBits.toString) > -1)

    }
  }}
}

case class TestPair(lower: Long, higher: Long)