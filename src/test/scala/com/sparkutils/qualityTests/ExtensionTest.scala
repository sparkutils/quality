package com.sparkutils.qualityTests

import com.sparkutils.quality.impl.extension.{AsUUIDFilter, ExtensionTesting, QualitySparkExtension}
import com.sparkutils.quality.impl.extension.QualitySparkExtension.disableRulesConf
import com.sparkutils.quality.utils.Testing
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, BinaryComparison, EqualTo, Equality, Or}
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.junit.{Before, Test}
import org.scalatest.FunSuite

import java.util.UUID

// including rowtools so standalone tests behave as if all of them are running and for verify compatibility
class ExtensionTest extends FunSuite with RowTools with TestUtils {

  @Before
  override def setup(): Unit = {
    cleanupOutput()
  }

  def wrapWithExtension(thunk: SparkSession => Unit): Unit = wrapWithExtensionT(thunk)

  def wrapWithExtensionT(thunk: SparkSession => Unit, disableConf: String = ""): Unit = {
    var tsparkSession: SparkSession = null

    try {
      try {
        sparkSessionF.close() // needed to stop the below being ignored
      } catch {
        case t: Throwable => fail("Could not shut down the wrapping spark", t)
      }
      System.setProperty(QualitySparkExtension.disableRulesConf, disableConf)
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
      } finally {
        System.clearProperty(QualitySparkExtension.disableRulesConf)
      }
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

  @Test
  def testExtensionDisableSpecific(): Unit = not2_4 {
    not_Databricks { // will never work on 2.4 and Databricks has a fixed session
      Testing.test {
        wrapWithExtensionT(tsparkSession => { }, AsUUIDFilter.getClass.getName)
      }
      val str = ExtensionTesting.disableRuleResult
      assert(str.indexOf(s"${disableRulesConf} = Set(${AsUUIDFilter.getClass.getName}) leaving List() remaining" ) > -1, s"str didn't have the expected contents, got $str")
    }
  }

  @Test
  def testExtensionDisableStar(): Unit = not2_4 {
    not_Databricks { // will never work on 2.4 and Databricks has a fixed session
      Testing.test {
        wrapWithExtensionT(tsparkSession => {}, "*")
      }
      val str = ExtensionTesting.disableRuleResult
      assert(str.indexOf(s"${disableRulesConf} = ") == -1, s"str did have an entry, should not have been logged, got $str")
    }
  }

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
  def testAsymmetricFilterPlanJoinEq(): Unit = not_Databricks {
    doTestAsymmetricFilterPlanJoin(wrapWithExtension _, "eq", (l, r) => l.===(r))
  }

  @Test
  def testAsymmetricFilterPlanJoinEqViaExistingSession(): Unit = onlyWithExtension {
    doTestAsymmetricFilterPlanJoin(wrapWithExistingSession _, "eq", (l, r) => l.===(r))
  }

  @Test
  def testAsymmetricFilterPlanJoinEQN(): Unit = not_Databricks {
    doTestAsymmetricFilterPlanJoin(wrapWithExtension _, "eqn", (l, r) => l.<=>(r))
  }

  @Test
  def testAsymmetricFilterPlanJoinEQNViaExistingSession(): Unit = onlyWithExtension {
    doTestAsymmetricFilterPlanJoin(wrapWithExistingSession _, "eqn", (l, r) => l.<=>(r))
  }

  @Test
  def testAsymmetricFilterPlanJoinLt(): Unit = not_Databricks {
    doTestAsymmetricFilterPlanJoin(wrapWithExtension _, "lt", (l, r) => l.<(r))
  }

  @Test
  def testAsymmetricFilterPlanJoinLtViaExistingSession(): Unit = onlyWithExtension {
    doTestAsymmetricFilterPlanJoin(wrapWithExistingSession _, "lt", (l, r) => l.<(r))
  }

  @Test
  def testAsymmetricFilterPlanJoinLte(): Unit = not_Databricks {
    doTestAsymmetricFilterPlanJoin(wrapWithExtension _, "lte", (l, r) => l.<=(r))
  }

  @Test
  def testAsymmetricFilterPlanJoinLteViaExistingSession(): Unit = onlyWithExtension {
    doTestAsymmetricFilterPlanJoin(wrapWithExistingSession _, "lte", (l, r) => l.<=(r))
  }

  @Test
  def testAsymmetricFilterPlanJoinGt(): Unit = not_Databricks {
    doTestAsymmetricFilterPlanJoin(wrapWithExtension _, "gt", (l, r) => l.>(r))
  }

  @Test
  def testAsymmetricFilterPlanJoinGtViaExistingSession(): Unit = onlyWithExtension {
    doTestAsymmetricFilterPlanJoin(wrapWithExistingSession _, "gt", (l, r) => l.>(r))
  }


  @Test
  def testAsymmetricFilterPlanJoinGte(): Unit = not_Databricks {
    doTestAsymmetricFilterPlanJoin(wrapWithExtension _, "gte", (l, r) => l.>=(r))
  }

  @Test
  def testAsymmetricFilterPlanJoinGteViaExistingSession(): Unit = onlyWithExtension {
    doTestAsymmetricFilterPlanJoin(wrapWithExistingSession _, "gte", (l, r) => l.>=(r))
  }

  def doTestAsymmetricFilterPlanJoin(viaExtension: (SparkSession => Unit) => Unit, hint: String,
                                     joinOp: (Column, Column) => Column): Unit =
    doTestAsymmetricFilterPlan(viaJoinOnContext(joinOp), Seq(
      (s" '${theuuid + 6}' = acontext", theuuid + "6", s"expr_rhs $hint"),
      (s" acontext = '${theuuid + 6}'", theuuid + "6", s"expr_lhs $hint"),
      (s" '${theuuid + 6}' = acontext and bhigher > 0", theuuid + "6", s"expr_rhs with further filter $hint"),
      (s" acontext = '${theuuid + 6}' and bhigher > 0", theuuid + "6", s"expr_lhs with further filter $hint"),
      (s" acontext > '${theuuid + 6}' and bhigher > 0", theuuid + "6", s"expr_lhs gt with further filter $hint")
    ), true, viaExtension = viaExtension)

  val viaJoinOnContext = (comp: (Column, Column) => Column) => (tsparkSession: SparkSession) => {
    val aWithContext = uuidPairsWithContext("a")(tsparkSession)
    val bWithContext = uuidPairsWithContext("b")(tsparkSession)
    import tsparkSession.implicits._
    aWithContext.join(bWithContext, comp($"acontext" , $"bcontext"))
  }

  def doTestAsymmetricFilterPlan(withContextF: SparkSession => DataFrame, filters: Seq[(String, String, String)],
                                 joinTest: Boolean = false, viaExtension: (SparkSession => Unit) => Unit = wrapWithExtension _): Unit = not2_4 {
    viaExtension { tsparkSession: SparkSession =>
      val withcontext = withContextF(tsparkSession)

      filters.foreach{ case (filter, expectedUUID, hint) =>
        val ds = withcontext.filter(filter)

        def assertWithPlan(condition: Boolean, hint: Any) = {
          if (!condition) {
            println(s"<---- filter was $filter")
            ds.explain(true)
          }
          assert(condition, hint)
        }

        val pushdowns = SparkTestUtils.getPushDowns( ds.queryExecution.executedPlan )
        if (pushdowns.isEmpty) {
          ds.explain(true)
        }

        def verify(pusheddown: String): Boolean = {
          val uu = java.util.UUID.fromString(expectedUUID)
          // for equals
          pusheddown.indexOf(uu.getLeastSignificantBits.toString) > -1 &&
          pusheddown.indexOf(uu.getMostSignificantBits.toString) > -1
        }

        // although we are only testing for one side in the join test spark will propagate the filter to both sides
        if (joinTest) {
          // verify that the join itself was re-written
          /*
          == Optimized Logical Plan ==
          Join Inner, ((alower#13L = blower#34L) AND (ahigher#14L = bhigher#35L))
          or for > than
          (((ahigher#14L = bhigher#36L) AND (alower#13L > blower#35L)) OR (ahigher#14L > bhigher#36L))
           */

          val res =
            ds.queryExecution.optimizedPlan.collect {
              case j: Join =>
                j.condition.flatMap{
                  case And(Equality(alower: Attribute, blower: Attribute),Equality(ahigher: Attribute, bhigher: Attribute))
                    if alower.name == "alower" && blower.name == "blower" &&
                      ahigher.name == "ahigher" && bhigher.name == "bhigher" =>
                    Some(true)
                  case Or(And(EqualTo(achigher: Attribute, bchigher: Attribute),
                    a@BinaryComparison(alower: Attribute, blower: Attribute)), b@BinaryComparison(ahigher: Attribute, bhigher: Attribute))
                    if alower.name == "alower" && blower.name == "blower" &&
                      ahigher.name == "ahigher" && bhigher.name == "bhigher" &&
                      achigher.name == "ahigher" && bchigher.name == "bhigher" &&
                      a.getClass.getName == b.getClass.getName =>
                    Some(true)
                  case _ => None
                }
            }.flatten
          assertWithPlan(res.nonEmpty, s"$hint - did not have re-written join")

          // both sides should have pushdown for equals, but for gt,lt etc. it'll be one sided for some, not for others
          assertWithPlan(pushdowns.nonEmpty, hint)
        } else
          assertWithPlan(pushdowns.size == 1, hint)

        assertWithPlan(pushdowns.exists(p => verify(p)), s"$hint - did not have a pushdown with the lower and higher uuid predicates but $pushdowns")
      }
    }
  }
}

case class TestPair(lower: Long, higher: Long)