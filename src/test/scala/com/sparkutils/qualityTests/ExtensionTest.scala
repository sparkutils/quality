package com.sparkutils.qualityTests

import com.sparkutils.quality.impl.extension.{AsUUIDFilter, ExtensionTesting, IDBase64Filter, QualitySparkExtension}
import com.sparkutils.quality.impl.extension.QualitySparkExtension.disableRulesConf
import com.sparkutils.quality.utils.Testing
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, BinaryComparison, EqualTo, Equality, Expression, Or}
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.sources.{Filter, EqualTo => SEqualTo, GreaterThanOrEqual => SGreaterThanOrEqual, GreaterThan => SGreaterThan, In => SIn, And => SAnd, Or => SOr}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.junit.{Before, Test}
import org.scalatest.FunSuite

import java.util.UUID

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry

// including rowtools so standalone tests behave as if all of them are running and for verify compatibility
class ExtensionTest extends FunSuite with TestUtils {

  @Before
  override def setup(): Unit = {
    cleanupOutput() // weirdly doesn't always run on databricks so we have test failures as a result.
  }

  def wrapWithExtension(thunk: SparkSession => Unit): Unit = wrapWithExtensionT(thunk)

  def wrapWithExtensionT(thunk: SparkSession => Unit, disableConf: String = "", forceInjection: String = null): Unit = {
    var tsparkSession: SparkSession = null

    try {
      try {
        sparkSessionF.close() // needed to stop the below being ignored
      } catch {
        case t: Throwable => fail("Could not shut down the wrapping spark", t)
      }
      System.setProperty(QualitySparkExtension.disableRulesConf, disableConf)
      if (forceInjection eq null)
        System.clearProperty(QualitySparkExtension.forceInjectFunction)
      else
        System.setProperty(QualitySparkExtension.forceInjectFunction, forceInjection)

      // attempt to create a new session
      tsparkSession = registerFS(SparkSession.builder()).config("spark.master", s"local[$hostMode]").config("spark.ui.enabled", false).
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
        System.clearProperty(QualitySparkExtension.forceInjectFunction)
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
      assert(str.indexOf(s"${disableRulesConf} = Set(${AsUUIDFilter.getClass.getName}) leaving List(${IDBase64Filter.getClass.getName}) remaining" ) > -1, s"str didn't have the expected contents, got $str")
    }
  }

  @Test
  def testExtensionDisableStar(): Unit = not2_4 {
    not_Databricks { // will never work on 2.4 and Databricks has a fixed session
      Testing.test {
        wrapWithExtensionT(tsparkSession => {}, "*")
      }
      val str = ExtensionTesting.disableRuleResult
      assert(str.isEmpty, s"should have been empty, got $str")
    }
  }

  val createview = (sparkSession: SparkSession) => {
    sparkSession.sql(s"create or replace view testfunctionview as select as_uuid($lower, $higher) context");
    import sparkSession.implicits._
    val res = sparkSession.sql("select context from testfunctionview").as[String].collect()
    assert(res.length == 1)
    assert(res.head == (theuuid+"6"))
    ()
  }

  @Test
  def testForceFunctionInjection(): Unit = not2_4 {
    not_Databricks { // will never work on 2.4 and Databricks has a fixed session
      // need to clear the existing quality functions out first
      com.sparkutils.quality.registerQualityFunctions(
        registerFunction = (str: String, f: Seq[Expression] => Expression) => FunctionRegistry.builtin.dropFunction(FunctionIdentifier(str))
      )

      Testing.test {
        try {
          wrapWithExtensionT(createview, forceInjection = "true")
          fail("expected to fail as the functions are temporary only")
        } catch {
          case throwable: Throwable if throwable.getMessage.contains("as_uuid") => ()
        }
      }
    }
  }

  @Test
  def testDefaultFunctionRegistrationViaBuiltIn(): Unit = not2_4 {
    not_Databricks { // will never work on 2.4 and Databricks has a fixed session
      Testing.test {
        wrapWithExtensionT(createview)
      }
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

  val theuuid6HigherNoA = SEqualTo("higher", 1314564453825188563L)
  def doAsymmetricFilterPlanCall(viaExtension: (SparkSession => Unit) => Unit = wrapWithExtension _): Unit = {
    val uu = java.util.UUID.fromString(theuuid + 6)
    doTestAsymmetricFilterPlan(uuidPairsWithContext(""), Seq(
      (s" '${theuuid + 6}' = context", theuuid6HigherNoA, "expr_rhs"),
      (s" context = '${theuuid + 6}'", theuuid6HigherNoA, "expr_lhs"),
      (s" '${theuuid + 6}' = context and lower < 0", theuuid6HigherNoA, "expr_rhs with further filter"),
      (s" context = '${theuuid + 6}' and lower < 0", theuuid6HigherNoA, "expr_lhs with further filter"),
      (s" context in ('${theuuid + 6}', '${theuuid + 4}')",
        SIn("lower", Array(java.util.UUID.fromString(theuuid + "6").getLeastSignificantBits,
          java.util.UUID.fromString(theuuid + "4").getLeastSignificantBits)), "with in")
    ), viaExtension = viaExtension)
  }

  val uuidPairsWithContext = (prefix: String) => (tsparkSession: SparkSession) => {
    import tsparkSession.implicits._

    val therows = for (i <- 0 until 10) yield {
      val uuid = theuuid + i
      val uuidobj = java.util.UUID.fromString(uuid)
      val lower = uuidobj.getLeastSignificantBits
      val higher = uuidobj.getMostSignificantBits
      TestRow(lower, higher, uuid)
    }

    // if this is not read from file a LocalRelation will be used and there is no Filter to be pushed down
    therows.toDS.selectExpr(s"lower as ${prefix}lower", s"higher as ${prefix}higher", s"asString as ${prefix}asString").write.mode("overwrite").parquet(outputDir + s"/${prefix}asymfilter")

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

  val higher = 1314564453825188563L
  val lower = -6605018797301088250L

  val theuuid6Higher = SEqualTo("ahigher", higher)

  def doTestAsymmetricFilterPlanJoin(viaExtension: (SparkSession => Unit) => Unit, hint: String,
                                     joinOp: (Column, Column) => Column): Unit =
    doTestAsymmetricFilterPlan(viaJoinOnContext(joinOp), Seq(
      (s" '${theuuid + 6}' = acontext", theuuid6Higher, s"expr_rhs $hint"),
      (s" acontext = '${theuuid + 6}'", theuuid6Higher, s"expr_lhs $hint"),
      (s" '${theuuid + 6}' = acontext and bhigher > 0", theuuid6Higher, s"expr_rhs with further filter $hint"),
      (s" acontext = '${theuuid + 6}' and bhigher > 0", theuuid6Higher, s"expr_lhs with further filter $hint"),
      (s" acontext > '${theuuid + 6}' and bhigher > 0",
        SOr(SAnd(SEqualTo("ahigher",higher),
          SGreaterThan("alower",lower)),
          SGreaterThan("ahigher",higher)), s"expr_lhs gt with further filter $hint")
    ), true, viaExtension = viaExtension)

  val viaJoinOnContext = (comp: (Column, Column) => Column) => (tsparkSession: SparkSession) => {
    val aWithContext = uuidPairsWithContext("a")(tsparkSession)
    val bWithContext = uuidPairsWithContext("b")(tsparkSession)
    import tsparkSession.implicits._
    aWithContext.join(bWithContext, comp($"acontext" , $"bcontext"))
  }
/*
  def verifyUUID(pusheddown: String, expectedContent: String): Boolean = {
    val uu = java.util.UUID.fromString(expectedContent)
    // for equals
    pusheddown.indexOf(uu.getLeastSignificantBits.toString) > -1 &&
      pusheddown.indexOf(uu.getMostSignificantBits.toString) > -1
  } */

  /*
  == Optimized Logical Plan ==
  Join Inner, ((alower#13L = blower#34L) AND (ahigher#14L = bhigher#35L))
  or for > than
  (((ahigher#14L = bhigher#36L) AND (alower#13L > blower#35L)) OR (ahigher#14L > bhigher#36L))
   */
  def verifyJoinPlanUUID(ds: DataFrame): Boolean =
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
    }.flatten.nonEmpty

  def doTestAsymmetricFilterPlan(withContextF: SparkSession => DataFrame, filters: Seq[(String, Filter, String)],
                                 joinTest: Boolean = false, viaExtension: (SparkSession => Unit) => Unit = wrapWithExtension _,
                                 verifyJoinPlan: DataFrame => Boolean = verifyJoinPlanUUID(_)
                                ): Unit = not2_4 {
    viaExtension { tsparkSession: SparkSession =>
      val withcontext = withContextF(tsparkSession)

      filters.foreach{ case (filter, expectedFilter, hint) =>
        val ds = withcontext.filter(filter)

        def assertWithPlan(condition: Boolean, hint: Any) = {
          if (!condition) {
            println(s"<---- filter was $filter")
            ds.explain(true)
          }
          assert(condition, hint)
        }

        val pushdowns = getPushDowns( ds.queryExecution.executedPlan )

        // with joins both sides should have pushdown for equals, but for gt,lt etc. it'll be one sided for some, not for others
        assertWithPlan(pushdowns.nonEmpty, s"$hint - did not have any pushed down filters")

        // although we are only testing for one side in the join test spark will propagate the filter to both sides
        if (joinTest) {
          // verify that the join itself was re-written
          assertWithPlan(verifyJoinPlan(ds), s"$hint - did not have re-written join")
        }

        assertWithPlan(pushdowns.contains(expectedFilter), s"$hint - did not have a pushdown with the correct predicates including $expectedFilter but $pushdowns")
      }
    }
  }

  /*
  Spark thankfully removes all the superfluous And(trues)
   */
  def verifyJoinPlanID(ds: DataFrame): Boolean =
    ds.queryExecution.optimizedPlan.collect {
      case j: Join =>
        j.condition.flatMap{
          case And(And(Equality(abase: Attribute, bbase: Attribute),Equality(ai0: Attribute, bi0: Attribute)),Equality(ai1: Attribute, bi1: Attribute))
            if abase.name == "abase" && bbase.name == "bbase" &&
              ai0.name == "ai0" && bi0.name == "bi0" &&
              ai1.name == "ai1" && bi1.name == "bi1" =>
            Some(true)
          case Or(And(And(EqualTo(abase: Attribute, bbase: Attribute), EqualTo(ai0: Attribute, bi0: Attribute)), a@BinaryComparison(ai1: Attribute, bi1: Attribute)),
            Or(And(EqualTo(abase1: Attribute, bbase1: Attribute), b@BinaryComparison(ai01: Attribute, bi01: Attribute)), c@BinaryComparison(abase2: Attribute, bbase2: Attribute)))
            if abase.name == "abase" && bbase.name == "bbase" &&
              ai0.name == "ai0" && bi0.name == "bi0" &&
              ai1.name == "ai1" && bi1.name == "bi1" &&
              abase1.name == "abase" && bbase1.name == "bbase" &&
              ai01.name == "ai0" && bi01.name == "bi0" &&
              abase2.name == "abase" && bbase2.name == "bbase" &&
              a.getClass.getName == b.getClass.getName &&
              a.getClass.getName == c.getClass.getName =>
            Some(true)
          case _ => None
        }
    }.flatten.nonEmpty

  val theSixthIDString = "AbRr/ChS6QAAAAAMA/hChwAAAAY="
  val testI1= 286051723926044678L
  val theSeventhIDString = "AbRr/ChS6QAAAAAMA/hChwAAAAc="
  val threeLongIDString = "AAAAAwAAAAAAAAB7AAAAAAAAMEQAAAAC39vnuA=="

  def doAsymmetricFilterPlanCallIdsFields(generator: SparkSession => DataFrame, viaExtension: (SparkSession => Unit) => Unit = wrapWithExtension _): Unit =
    doTestAsymmetricFilterPlan(generator, Seq(
      (s" '$theSixthIDString' = id", SEqualTo("i1",testI1), "expr_rhs"),
      (s" id = '$theSixthIDString'", SEqualTo("i1",testI1), "expr_lhs"),
      (s" '$theSixthIDString' = id and i1 > 286051723926044673L", SEqualTo("i1",testI1), "expr_rhs with further filter"),
      (s" id = '$theSixthIDString' and i1 > 286051723926044673L", SEqualTo("i1",testI1), "expr_lhs with further filter"),
      (s" id in ('$theSixthIDString', '$theSeventhIDString')", SIn("i1",Array(testI1, 286051723926044679L)), "with in")
    ), viaExtension = viaExtension, verifyJoinPlan = verifyJoinPlanID(_))
/*
+--------+-------------------+------------------+
|pre_base|             pre_i0|            pre_i1|
+--------+-------------------+------------------+
|28601340|2905640895816663052|286051723926044673|
+--------+-------------------+------------------+
 */

  val id_base = 28601340
  val id_i0 = 2905640895816663052L
  val id_i1 = 286051723926044673L

  val baseID = TestID(id_base, id_i0, id_i1)

  def genBase64(select: String, prefix: String, tsparkSession: SparkSession): DataFrame = {
    import tsparkSession.implicits._

    val therows = for (i <- 0 until 10) yield {
      baseID.copy( i1 = baseID.i1 + i)
    }

    // if this is not read from file a LocalRelation will be used and there is no Filter to be pushed down
    therows.toDS.selectExpr(s"base as ${prefix}base", s"i0 as ${prefix}i0", s"i1 as ${prefix}i1").write.mode("overwrite").parquet(outputDir + s"/${prefix}asymfilter")

    val reread = tsparkSession.read.parquet(outputDir + s"/${prefix}asymfilter")
    val withcontext = reread.selectExpr("*", select)
    withcontext
  }

  val idsWithContextFields = (prefix: String) => (tsparkSession: SparkSession) =>
    genBase64(s"id_base64(${prefix}base, ${prefix}i0, ${prefix}i1) as ${prefix}id", prefix, tsparkSession)

  @Test
  def testAsymmetricFilterPlanIdCallFields(): Unit = not_Databricks {
    doAsymmetricFilterPlanCallIdsFields( idsWithContextFields(""), wrapWithExtension _)
  }

  @Test
  def testAsymmetricFilterPlanIdCallFieldsViaExistingSession(): Unit = onlyWithExtension {
    doAsymmetricFilterPlanCallIdsFields( idsWithContextFields(""), wrapWithExistingSession _)
  }

  val idsWithContextStruct = (prefix: String) => (tsparkSession: SparkSession) =>
    genBase64(s"idbase64(named_struct('pre_base', ${prefix}base, 'pre_i0', ${prefix}i0, 'pre_i1',  ${prefix}i1)) as ${prefix}id", prefix, tsparkSession)

  val idsWithContextStructLarger = (prefix: String) => (tsparkSession: SparkSession) =>
    genBase64(s"idbase64(named_struct('pre_base', ${prefix}base, 'pre_i0', ${prefix}i0, 'pre_i1',  ${prefix}i1, 'pre_i2', 20033L)) as ${prefix}id", prefix, tsparkSession)
  val idsWithContextFieldsLarger = (prefix: String) => (tsparkSession: SparkSession) =>
    genBase64(s"id_base64(${prefix}base, ${prefix}i0, ${prefix}i1, 20033L) as ${prefix}id", prefix, tsparkSession)

  val viaJoinIDStructs = (comp: (Column, Column) => Column) => (tsparkSession: SparkSession) => {
    val aWithContext = idsWithContextStruct("a")(tsparkSession)
    val bWithContext = idsWithContextStruct("b")(tsparkSession)
    import tsparkSession.implicits._
    aWithContext.join(bWithContext, comp($"aid" , $"bid"))
  }

  val viaJoinIDFields = (comp: (Column, Column) => Column) => (tsparkSession: SparkSession) => {
    val aWithContext = idsWithContextFields("a")(tsparkSession)
    val bWithContext = idsWithContextFields("b")(tsparkSession)
    import tsparkSession.implicits._
    aWithContext.join(bWithContext, comp($"aid" , $"bid"))
  }

  val viaJoinIDsMixed = (comp: (Column, Column) => Column) => (tsparkSession: SparkSession) => {
    val aWithContext = idsWithContextFields("a")(tsparkSession)
    val bWithContext = idsWithContextStruct("b")(tsparkSession)
    import tsparkSession.implicits._
    aWithContext.join(bWithContext, comp($"aid" , $"bid"))
  }

  val viaJoinIDStructsLarger = (comp: (Column, Column) => Column) => (tsparkSession: SparkSession) => {
    val aWithContext = idsWithContextStruct("a")(tsparkSession)
    val bWithContext = idsWithContextStructLarger("b")(tsparkSession)
    import tsparkSession.implicits._
    aWithContext.join(bWithContext, comp($"aid" , $"bid"))
  }

  val viaJoinIDFieldsLarger = (comp: (Column, Column) => Column) => (tsparkSession: SparkSession) => {
    val aWithContext = idsWithContextFields("a")(tsparkSession)
    val bWithContext = idsWithContextFieldsLarger("b")(tsparkSession)
    import tsparkSession.implicits._
    aWithContext.join(bWithContext, comp($"aid" , $"bid"))
  }

  val viaJoinIDsMixedLarger = (comp: (Column, Column) => Column) => (tsparkSession: SparkSession) => {
    val aWithContext = idsWithContextFields("a")(tsparkSession)
    val bWithContext = idsWithContextStructLarger("b")(tsparkSession)
    import tsparkSession.implicits._
    aWithContext.join(bWithContext, comp($"aid" , $"bid"))
  }

  def doTestDifferentLengthsIdJoin(viaExtension: (SparkSession => Unit) => Unit, hint: String, generator: ((Column, Column) => Column) => SparkSession => DataFrame, joinOp: (Column, Column) => Column): Unit =
  // will trigger the IF clause and return false, so no records are found and, given no broken down part equals, no pushed down predicates either.
    try {doTestAsymmetricFilterPlan(generator(joinOp), Seq(
      (s" '$theSixthIDString' = aid", SEqualTo("aid",testI1), s"expr_rhs $hint")
    ), true, viaExtension = viaExtension, verifyJoinPlan = verifyJoinPlanID(_))
    } catch {
      case t: Throwable if anyCauseHas(t, _.getMessage().indexOf(" different sizes - did not have re-written join") > -1)=> ()
    }

  def doTestDifferentLengthsIdJoinAndFilter(viaExtension: (SparkSession => Unit) => Unit, hint: String, generator: ((Column, Column) => Column) => SparkSession => DataFrame): Unit =
  // will trigger the IF clause and return false, so no records are found and, given no broken down part equals, no pushed down predicates either.
    try {doTestAsymmetricFilterPlan(generator((l, r) => l.===(r)), Seq(
      (s" '$theSixthIDString' = aid", SEqualTo("aid",testI1), s"expr_rhs $hint")
    ), true, viaExtension = viaExtension, verifyJoinPlan = verifyJoinPlanID(_))
    } catch {
      case t: Throwable if anyCauseHas(t, _.getMessage().indexOf(" different sizes - did not have re-written join") > -1)=> ()
    }

  @Test
  def testAsymmetricFilterPlanIdJoinDifferentSizeStruct(): Unit = not_Databricks {
    doTestDifferentLengthsIdJoin(wrapWithExtension _, "structs different sizes",  viaJoinIDStructsLarger, (l, r) => l.===(r) )
  }

  @Test
  def testAsymmetricFilterPlanIdJoinDifferentSizeFields(): Unit = not_Databricks {
    doTestDifferentLengthsIdJoin(wrapWithExtension _, "fields different sizes",  viaJoinIDFieldsLarger, (l, r) => l.===(r) )
  }

  @Test
  def testAsymmetricFilterPlanIdJoinDifferentSizeMixed(): Unit = not_Databricks {
    doTestDifferentLengthsIdJoin(wrapWithExtension _, "mixed different sizes",  viaJoinIDsMixedLarger, (l, r) => l.===(r) )
  }

  @Test
  def testAsymmetricFilterPlanIdJoinDifferentSizeStructLT(): Unit = not_Databricks {
    doTestDifferentLengthsIdJoin(wrapWithExtension _, "structs different sizes",  viaJoinIDStructsLarger, (l, r) => l.<(r) )
  }

  @Test
  def testAsymmetricFilterPlanIdJoinDifferentSizeFieldsLT(): Unit = not_Databricks {
    doTestDifferentLengthsIdJoin(wrapWithExtension _, "fields different sizes",  viaJoinIDFieldsLarger, (l, r) => l.<(r) )
  }

  @Test
  def testAsymmetricFilterPlanIdJoinDifferentSizeMixedLT(): Unit = not_Databricks {
    doTestDifferentLengthsIdJoin(wrapWithExtension _, "mixed different sizes",  viaJoinIDsMixedLarger, (l, r) => l.<(r) )
  }

  @Test
  def testAsymmetricFilterPlanIdCallStructs(): Unit = not_Databricks {
    doAsymmetricFilterPlanCallIdsFields( idsWithContextStruct(""), wrapWithExtension _)
  }

  @Test
  def testAsymmetricFilterPlanIdCallStructsViaExistingSession(): Unit = onlyWithExtension {
    doAsymmetricFilterPlanCallIdsFields( idsWithContextStruct(""), wrapWithExistingSession _)
  }

  @Test
  def testDifferentLengthsId(): Unit = not_Databricks{
    // will trigger the IF clause and return false, so no records are found and, given no broken down part equals, no pushed down predicates either.
    try {
      doTestAsymmetricFilterPlan(idsWithContextStruct(""), Seq(
        (s" '$threeLongIDString' = id", SEqualTo("id",testI1), "expr_rhs")
      ), viaExtension = wrapWithExtension _, verifyJoinPlan = verifyJoinPlanID(_))
    } catch {
      case t: Throwable if anyCauseHas(t, _.getMessage().indexOf("expr_rhs - did not have any pushed down filters") > -1)=> ()
    }
  }

  def doTestAsymmetricFilterPlanJoinIDS(viaExtension: (SparkSession => Unit) => Unit, hint: String,
                                     joinOp: (Column, Column) => Column, generator: ((Column, Column) => Column) => SparkSession => DataFrame): Unit =
    doTestAsymmetricFilterPlan(generator(joinOp), Seq(
      (s" '$theSixthIDString' = aid", SEqualTo("ai1",testI1), s"expr_rhs $hint"),
      (s" aid = '$theSixthIDString'", SEqualTo("ai1",testI1), s"expr_lhs $hint"),
      (s" '$theSixthIDString' = aid and ai1 > 286051723926044673L", SEqualTo("ai1",testI1), s"expr_rhs with further filter $hint"),
      (s" aid = '$theSixthIDString' and ai1 > 286051723926044673L", SEqualTo("ai1",testI1), s"expr_lhs with further filter $hint"),
      (s" aid > '$theSixthIDString' and ai1 > 286051723926044673L",
        SOr(SAnd(SAnd(SEqualTo("abase", 28601340), SEqualTo("ai0", 2905640895816663052L)),
          SGreaterThan("ai1", 286051723926044678L)), SOr(SAnd(SEqualTo("abase", 28601340),
          SGreaterThan("ai0", 2905640895816663052L)), SGreaterThan("abase", 28601340))), s"expr_lhs gt with further filter $hint")
    ), true, viaExtension = viaExtension, verifyJoinPlan = verifyJoinPlanID(_))

  @Test
  def testAsymmetricFilterPlanJoinFieldsEq(): Unit = not_Databricks {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension _, "eq", (l, r) => l.===(r), viaJoinIDFields)
  }
  @Test
  def testAsymmetricFilterPlanJoinStructEq(): Unit = not_Databricks {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension _, "eq", (l, r) => l.===(r), viaJoinIDStructs)
  }
  @Test
  def testAsymmetricFilterPlanJoinMixedEq(): Unit = not_Databricks {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension _, "eq", (l, r) => l.===(r), viaJoinIDsMixed)
  }

  @Test
  def testAsymmetricFilterPlanJoinFieldsEqn(): Unit = not_Databricks {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension _, "eqn", (l, r) => l.<=>(r), viaJoinIDFields)
  }
  @Test
  def testAsymmetricFilterPlanJoinStructEqn(): Unit = not_Databricks {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension _, "eqn", (l, r) => l.<=>(r), viaJoinIDStructs)
  }
  @Test
  def testAsymmetricFilterPlanJoinMixedEqn(): Unit = not_Databricks {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension _, "eqn", (l, r) => l.<=>(r), viaJoinIDsMixed)
  }

  @Test
  def testAsymmetricFilterPlanJoinFieldsLt(): Unit = not_Databricks {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension _, "lt", (l, r) => l.<(r), viaJoinIDFields)
  }
  @Test
  def testAsymmetricFilterPlanJoinStructLt(): Unit = not_Databricks {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension _, "lt", (l, r) => l.<(r), viaJoinIDStructs)
  }
  @Test
  def testAsymmetricFilterPlanJoinMixedLt(): Unit = not_Databricks {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension _, "lt", (l, r) => l.<(r), viaJoinIDsMixed)
  }

  @Test
  def testAsymmetricFilterPlanJoinFieldsLtEq(): Unit = not_Databricks {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension _, "lte", (l, r) => l.<=(r), viaJoinIDFields)
  }
  @Test
  def testAsymmetricFilterPlanJoinStructLtEq(): Unit = not_Databricks {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension _, "lte", (l, r) => l.<=(r), viaJoinIDStructs)
  }
  @Test
  def testAsymmetricFilterPlanJoinMixedLtEq(): Unit = not_Databricks {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension _, "lte", (l, r) => l.<=(r), viaJoinIDsMixed)
  }

  @Test
  def testAsymmetricFilterPlanJoinFieldsGt(): Unit = not_Databricks {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension _, "gt", (l, r) => l.>(r), viaJoinIDFields)
  }
  @Test
  def testAsymmetricFilterPlanJoinStructGt(): Unit = not_Databricks {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension _, "gt", (l, r) => l.>(r), viaJoinIDStructs)
  }
  @Test
  def testAsymmetricFilterPlanJoinMixedGt(): Unit = not_Databricks {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension _, "gt", (l, r) => l.>(r), viaJoinIDsMixed)
  }

  @Test
  def testAsymmetricFilterPlanJoinFieldsGtEq(): Unit = not_Databricks {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension _, "gte", (l, r) => l.>=(r), viaJoinIDFields)
  }
  @Test
  def testAsymmetricFilterPlanJoinStructGtEq(): Unit = not_Databricks {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension _, "gte", (l, r) => l.>=(r), viaJoinIDStructs)
  }
  @Test
  def testAsymmetricFilterPlanJoinMixedGtEq(): Unit = not_Databricks {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension _, "gte", (l, r) => l.>=(r), viaJoinIDsMixed)
  }


  @Test
  def testAsymmetricFilterPlanJoinFieldsEqViaExistingSession(): Unit = onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession _, "eq", (l, r) => l.===(r), viaJoinIDFields)
  }
  @Test
  def testAsymmetricFilterPlanJoinStructEqViaExistingSession(): Unit = onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession _, "eq", (l, r) => l.===(r), viaJoinIDStructs)
  }
  @Test
  def testAsymmetricFilterPlanJoinMixedEqViaExistingSession(): Unit = onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession _, "eq", (l, r) => l.===(r), viaJoinIDsMixed)
  }

  @Test
  def testAsymmetricFilterPlanJoinFieldsEqnViaExistingSession(): Unit = onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession _, "eqn", (l, r) => l.<=>(r), viaJoinIDFields)
  }
  @Test
  def testAsymmetricFilterPlanJoinStructEqnViaExistingSession(): Unit = onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession _, "eqn", (l, r) => l.<=>(r), viaJoinIDStructs)
  }
  @Test
  def testAsymmetricFilterPlanJoinMixedEqnViaExistingSession(): Unit = onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession _, "eqn", (l, r) => l.<=>(r), viaJoinIDsMixed)
  }

  @Test
  def testAsymmetricFilterPlanJoinFieldsLtViaExistingSession(): Unit = onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession _, "lt", (l, r) => l.<(r), viaJoinIDFields)
  }
  @Test
  def testAsymmetricFilterPlanJoinStructLtViaExistingSession(): Unit = onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession _, "lt", (l, r) => l.<(r), viaJoinIDStructs)
  }
  @Test
  def testAsymmetricFilterPlanJoinMixedLtViaExistingSession(): Unit = onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession _, "lt", (l, r) => l.<(r), viaJoinIDsMixed)
  }

  @Test
  def testAsymmetricFilterPlanJoinFieldsLtEqViaExistingSession(): Unit = onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession _, "lte", (l, r) => l.<=(r), viaJoinIDFields)
  }
  @Test
  def testAsymmetricFilterPlanJoinStructLtEqViaExistingSession(): Unit = onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession _, "lte", (l, r) => l.<=(r), viaJoinIDStructs)
  }
  @Test
  def testAsymmetricFilterPlanJoinMixedLtEqViaExistingSession(): Unit = onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession _, "lte", (l, r) => l.<=(r), viaJoinIDsMixed)
  }

  @Test
  def testAsymmetricFilterPlanJoinFieldsGtViaExistingSession(): Unit = onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession _, "gt", (l, r) => l.>(r), viaJoinIDFields)
  }
  @Test
  def testAsymmetricFilterPlanJoinStructGtViaExistingSession(): Unit = onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession _, "gt", (l, r) => l.>(r), viaJoinIDStructs)
  }
  @Test
  def testAsymmetricFilterPlanJoinMixedGtViaExistingSession(): Unit = onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession _, "gt", (l, r) => l.>(r), viaJoinIDsMixed)
  }

  @Test
  def testAsymmetricFilterPlanJoinFieldsGtEqViaExistingSession(): Unit = onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession _, "gte", (l, r) => l.>=(r), viaJoinIDFields)
  }
  @Test
  def testAsymmetricFilterPlanJoinStructGtEqViaExistingSession(): Unit = onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession _, "gte", (l, r) => l.>=(r), viaJoinIDStructs)
  }
  @Test
  def testAsymmetricFilterPlanJoinMixedGtEqViaExistingSession(): Unit = onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession _, "gte", (l, r) => l.>=(r), viaJoinIDsMixed)
  }
/*
attempts under #19 doesn't seem possible, keeping here for reference

  @Test
  def testPushdownOnString(): Unit = wrapWithExtension {
    session =>
      // PushedFilters: [IsNotNull(pasString), EqualTo(pasString,123e4567-e89b-12d3-a456-426614174006)],
//      val ds = uuidPairsWithContext("p").apply(session).filter(s"pasString = '${ theuuid + "6" }' ")

      val uuid = theuuid + "6"
      val uuidobj = java.util.UUID.fromString(uuid)
      val lower = uuidobj.getLeastSignificantBits
      val higher = uuidobj.getMostSignificantBits
//endsWith(pasString, '6')
      val ds = uuidPairsWithContext("p").apply(session).filter(s"pasString = as_uuid(plower, phigher) and pasString = '${ theuuid + "6" }' ")
      ds.explain(true)
    ()
  }
*/
}

case class TestRow(lower: Long, higher: Long, asString: String)
case class TestID(base: Int, i0: Long, i1: Long)