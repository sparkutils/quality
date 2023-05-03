package com.sparkutils.qualityTests

import com.sparkutils.quality

import java.util.UUID
import com.sparkutils.quality.impl.extension.{AsUUIDFilter, QualitySparkExtension}
import com.sparkutils.quality.registerQualityFunctions
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, EqualTo}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
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

  val theuuid = "123e4567-e89b-12d3-a456-42661417400"

  // pretty much only for databricks
  def wrapWithExistingSession(thunk: SparkSession => Unit): Unit = {
    val tsparkSession = sparkSessionF
    thunk(tsparkSession)
  }

  @Test
  def testAsymmetricFilterPlan(): Unit = not_Databricks { // will never work on 2.4 and Databricks has a fixed session
    doAsymmetricFilterPlanCall()
  }

  @Test
  def testAsymmetricFilterPlanViaExistingSession(): Unit = onlyWithExtension {
    doAsymmetricFilterPlanCall(wrapWithExistingSession _)
  }

  def doAsymmetricFilterPlanCall(viaExtension: (SparkSession => Unit) => Unit = wrapWithExtension _): Unit = {
    val uu = java.util.UUID.fromString(theuuid + 6)
    doTestAsymmetricFilterPlan(uuidPairsWithContext(""), Seq(
      (s" '${theuuid + 6}' = context", theuuid + "6", "expr_rhs"),
      (s" context = '${theuuid + 6}'", theuuid + "6", "expr_lhs"),
      (s" '${theuuid + 6}' = context and lower < 0", theuuid + "6", "expr_rhs with further filter"),
      (s" context = '${theuuid + 6}' and lower < 0", theuuid + "6", "expr_lhs with further filter"),
      (s" context in ('${theuuid + 6}', '${theuuid + 4}')", theuuid + "6", "with in")
    ), viaExtension = viaExtension)
  }

  val uuidPairsWithContext = (prefix: String) => (tsparkSession: SparkSession) => {
    import tsparkSession.implicits._

    val therows = for (i <- 0 until 10) yield {
      val uuid = theuuid + i
      val uuidobj = java.util.UUID.fromString(uuid)
      val lower = uuidobj.getLeastSignificantBits
      val higher = uuidobj.getMostSignificantBits
      TestPair(lower, higher)
    }

    // if this is not read from file a LocalRelation will be used and there is no Filter to be pushed down
    therows.toDS.selectExpr(s"lower as ${prefix}lower", s"higher as ${prefix}higher").write.parquet(outputDir + s"/${prefix}asymfilter")

    val reread = tsparkSession.read.parquet(outputDir + s"/${prefix}asymfilter")
    val withcontext = reread.selectExpr("*", s"as_uuid(${prefix}lower, ${prefix}higher) as ${prefix}context")
    withcontext
  }

  @Test
  def testAsymmetricFilterPlanJoin(): Unit = not_Databricks {
    doTestAsymmetricFilterPlanJoin()
  }

  @Test
  def testAsymmetricFilterPlanJoinViaExistingSession(): Unit = onlyWithExtension {
    doTestAsymmetricFilterPlanJoin(wrapWithExistingSession _)
  }

  def doTestAsymmetricFilterPlanJoin(viaExtension: (SparkSession => Unit) => Unit = wrapWithExtension _): Unit =
    doTestAsymmetricFilterPlan(viaJoinOnContext, Seq(
      (s" '${theuuid + 6}' = acontext", theuuid + "6", "expr_rhs"),
      (s" acontext = '${theuuid + 6}'", theuuid + "6", "expr_lhs"),
      (s" '${theuuid + 6}' = acontext and bhigher > 0", theuuid + "6", "expr_rhs with further filter"),
      (s" acontext = '${theuuid + 6}' and bhigher > 0", theuuid + "6", "expr_lhs with further filter")
    ), true, viaExtension = viaExtension)

  val viaJoinOnContext = (tsparkSession: SparkSession) => {
    val aWithContext = uuidPairsWithContext("a")(tsparkSession)
    val bWithContext = uuidPairsWithContext("b")(tsparkSession)
    import tsparkSession.implicits._
    aWithContext.join(bWithContext, $"acontext" === $"bcontext")
  }

  def doTestAsymmetricFilterPlan(withContextF: SparkSession => DataFrame, filters: Seq[(String, String, String)],
                                 joinTest: Boolean = false, viaExtension: (SparkSession => Unit) => Unit = wrapWithExtension _): Unit = not2_4 {
    viaExtension { tsparkSession: SparkSession =>
      val withcontext = withContextF(tsparkSession)

      filters.foreach{ case (filter, expectedUUID, hint) =>
        val ds = withcontext.filter(filter)

        val pushdowns = SparkTestUtils.getPushDowns( ds.queryExecution.executedPlan )
        if (pushdowns.isEmpty) {
          ds.explain(true)
        }

        def verify(pusheddown: String): Unit = {
          //println(s"pushed down $pusheddown")
          //assert(pusheddown != "[]", s"$hint - The predicates were not pushed down")

          val uu = java.util.UUID.fromString(expectedUUID)
          assert(pusheddown.indexOf(uu.getLeastSignificantBits.toString) > -1, s"$hint - did not have lower uuid predicate")
          assert(pusheddown.indexOf(uu.getMostSignificantBits.toString) > -1, s"$hint - did not have higher uuid predicate")
        }

        // although we are only testing for one side in the join test spark will propagate the filter to both sides
        if (joinTest) {
          // verify that the join itself was re-written
          /*
          == Optimized Logical Plan ==
          Join Inner, ((alower#13L = blower#34L) AND (ahigher#14L = bhigher#35L))
           */

          val res =
            ds.queryExecution.optimizedPlan.collect {
              case j: Join =>
                j.condition.flatMap{
                  case And(EqualTo(alower: Attribute, blower: Attribute),EqualTo(ahigher: Attribute, bhigher: Attribute))
                    if alower.name == "alower" && blower.name == "blower" &&
                      ahigher.name == "ahigher" && bhigher.name == "bhigher"
                  =>
                    Some(true)
                  case _ => None
                }
            }.flatten
          assert(res.nonEmpty, s"$hint - did not have re-written join")

          // both sides should have pushdown
          assert(pushdowns.size == 2, hint)
        } else
          assert(pushdowns.size == 1, hint)

        pushdowns.foreach(p => verify(p))
      }
    }
  }
}

case class TestPair(lower: Long, higher: Long)