package com.sparkutils.qualityTests.classicOnly

import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import com.sparkutils.quality.classicFunctions.registerQualityFunctions
import com.sparkutils.quality.impl.extension.QualitySparkExtension.disableRulesConf
import com.sparkutils.quality.impl.extension._
import com.sparkutils.qualityTests.util.ClassicSharedTests
import com.sparkutils.testing.TestUtils.anyCauseHas
import com.sparkutils.testing.{ClassicSparkTestUtils, ClassicTestUtils}
import org.apache.hadoop.fs.local.BareStreamingLocalFileSystem
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, BinaryComparison, EqualTo, Equality, Expression, Or}
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.sources.{Filter, And => SAnd, EqualTo => SEqualTo, GreaterThan => SGreaterThan, In => SIn, Or => SOr}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.io.File
import java.util.UUID

// including rowtools so standalone tests behave as if all of them are running and for verify compatibility
abstract class ExtensionTestBase extends ClassicSharedTests  {

  def shouldRun: Boolean

  def when_not_disabled(thunk: => Unit): Unit =
    if (shouldRun) {
      thunk
   }

  def wrapWithExtension(thunk: SparkSession => Unit): Unit = wrapWithExtensionT(thunk)

  def wrapWithExtensionT(thunk: SparkSession => Unit, disableConf: String = "", forceInjection: String = null, withHive: Boolean = false): Unit = {
    var tsparkSession: SparkSession = null

    try {
      swapSession {
        if (withHive) {
          cleanUp("./metastore_db")
          cleanUp("./spark-warehouse")
        }

        try {
          System.setProperty(QualitySparkExtension.testingConf, "testing")
          System.setProperty(QualitySparkExtension.disableRulesConf, disableConf)
          if (forceInjection eq null)
            System.clearProperty(QualitySparkExtension.forceInjectFunction)
          else
            System.setProperty(QualitySparkExtension.forceInjectFunction, forceInjection)

          val enableHive = (builder: SparkSession.Builder) =>
            if (withHive)
              builder.enableHiveSupport()
            else
              builder

          val enableDelta = (builder: SparkSession.Builder) =>
            if (format == "delta")
              builder.config("spark.sql.extensions", classOf[QualitySparkExtension].getName() + ",io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            else
              builder

          // attempt to create a new session
          tsparkSession = enableDelta(enableHive(
            {
              val builder = SparkSession.builder()
              if (System.getProperty("os.name").startsWith("Windows"))
                builder.config("spark.hadoop.fs.file.impl", classOf[BareLocalFileSystem].getName).
                  config("spark.hadoop.fs.AbstractFileSystem.file.impl", classOf[BareStreamingLocalFileSystem].getName)
              else
                builder
            }
              .config("spark.master", s"local[$classicHostMode]").config("spark.ui.enabled", false).
              config("spark.sql.extensions", classOf[QualitySparkExtension].getName())))
            .getOrCreate()
          tsparkSession.sparkContext.setLogLevel("ERROR")

          thunk(tsparkSession)
        } finally {
            if (tsparkSession ne null) {
              tsparkSession.close()
            }
        }
      }

    } finally {
      System.clearProperty(QualitySparkExtension.disableRulesConf)
      System.clearProperty(QualitySparkExtension.forceInjectFunction)

      if (format == "delta") {
        // https://github.com/delta-io/delta/issues/629 workaround
        org.apache.spark.sql.delta.DeltaLog.clearCache()
      }
    }

  }

  test("testExtension") { when_not_disabled {
    not_Cluster { // will never work on 2.4 and Databricks has a fixed session
      wrapWithExtension { tsparkSession =>
        import tsparkSession.implicits._

        val orig = UUID.randomUUID()
        val uuid = orig.toString

        val res = tsparkSession.sql(s"select longPairFromUUID('$uuid') as fparts").selectExpr("as_uuid(fparts.lower, fparts.higher) as asUUIDExpr")
        val sres = res.as[String].head()
        assert(sres == uuid)
      }
    }
  } }

  test("testExtensionDisableSpecific") { when_not_disabled {
    not_Cluster { // will never work on 2.4 and Databricks has a fixed session
      wrapWithExtensionT(tsparkSession => {}, AsUUIDFilter.getClass.getName)
      val str = ExtensionTesting.disableRuleResult
      assert(str.indexOf(s"${disableRulesConf} = Set(${AsUUIDFilter.getClass.getName}) leaving List(${IDBase64Filter.getClass.getName}, ${FunNRewrite.getClass.getName}) remaining") > -1, s"str didn't have the expected contents, got $str")
    }
  } }

  test("testExtensionDisableStar") { when_not_disabled {
    not_Cluster { // will never work on 2.4 and Databricks has a fixed session
      wrapWithExtensionT(tsparkSession => {}, "*")
      val str = ExtensionTesting.disableRuleResult
      assert(str.isEmpty, s"should have been empty, got $str")
    }
  } }

  val createview = (sparkSession: SparkSession) => {
    sparkSession.sql(s"create or replace view testfunctionview as select as_uuid($lower, $higher) context");
    val s = sparkSession
    import s.implicits._
    val res = sparkSession.sql("select context from testfunctionview").as[String].collect()
    assert(res.length == 1)
    assert(res.head == (theuuid + "6"))
    ()
  }

  test("testForceFunctionInjection") { when_not_disabled {
    not_Cluster { // will never work on 2.4 and Databricks has a fixed session
      // need to clear the existing quality functions out first
      registerQualityFunctions(
        registerFunction = (str: String, f: Seq[Expression] => Expression) => FunctionRegistry.builtin.dropFunction(FunctionIdentifier(str))
      )

      try {
        wrapWithExtensionT(createview, forceInjection = "true")
        fail("expected to fail as the functions are temporary only")
      } catch {
        case throwable: Throwable =>
          if (throwable.getMessage.contains("as_uuid"))  ()
      }
    }
  } }

  test("testDefaultFunctionRegistrationViaBuiltIn") { when_not_disabled {
    not_Cluster { // will never work on 2.4 and Databricks has a fixed session
      wrapWithExtensionT(createview)
    }
  } }


  val theuuid = "123e4567-e89b-12d3-a456-42661417400"

  // pretty much only for databricks
  def wrapWithExistingSession(thunk: SparkSession => Unit): Unit = {
    val tsparkSession = sparkSession
    registerQualityFunctions()

    thunk(tsparkSession)
  }

  test("testAsymmetricFilterPlan") { when_not_disabled { not_Cluster { // will never work on 2.4 and Databricks has a fixed session
    doAsymmetricFilterPlanCall()
  } } }

  test("testAsymmetricFilterPlanViaExistingSession") { when_not_disabled {  onlyWithExtension {
    doAsymmetricFilterPlanCall(wrapWithExistingSession)
  } } }

  val theuuid6HigherNoA = SEqualTo("higher", 1314564453825188563L)

  def doAsymmetricFilterPlanCall(viaExtension: (SparkSession => Unit) => Unit = wrapWithExtension): Unit = {
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

  def format: String

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
    therows.toDS().selectExpr(s"lower as ${prefix}lower", s"higher as ${prefix}higher", s"asString as ${prefix}asString").write.mode("overwrite").format(format).save(outputDir + s"/${format}_${prefix}asymfilter")

    val reread = tsparkSession.read.format(format).load(outputDir + s"/${format}_${prefix}asymfilter")
    val withcontext = reread.selectExpr("*", s"as_uuid(${prefix}lower, ${prefix}higher) as ${prefix}context")
    withcontext
  }

  test("testAsymmetricFilterPlanJoinEq") { not_Cluster {
    doTestAsymmetricFilterPlanJoin(wrapWithExtension, "eq", (l, r) => l.===(r))
  } }

  test("testAsymmetricFilterPlanJoinEqViaExistingSession") { onlyWithExtension {
    doTestAsymmetricFilterPlanJoin(wrapWithExistingSession, "eq", (l, r) => l.===(r))
  }}

  test("testAsymmetricFilterPlanJoinEQN") { not_Cluster {
    doTestAsymmetricFilterPlanJoin(wrapWithExtension, "eqn", (l, r) => l.<=>(r))
  }}

  test("testAsymmetricFilterPlanJoinEQNViaExistingSession") { onlyWithExtension {
    doTestAsymmetricFilterPlanJoin(wrapWithExistingSession, "eqn", (l, r) => l.<=>(r))
  }}

  test("testAsymmetricFilterPlanJoinLt") { not_Cluster {
    doTestAsymmetricFilterPlanJoin(wrapWithExtension, "lt", (l, r) => l.<(r))
  }}

  test("testAsymmetricFilterPlanJoinLtViaExistingSession") { onlyWithExtension {
    doTestAsymmetricFilterPlanJoin(wrapWithExistingSession, "lt", (l, r) => l.<(r))
  }}

  test("testAsymmetricFilterPlanJoinLte") { not_Cluster {
    doTestAsymmetricFilterPlanJoin(wrapWithExtension, "lte", (l, r) => l.<=(r))
  }}

  test("testAsymmetricFilterPlanJoinLteViaExistingSession") { onlyWithExtension {
    doTestAsymmetricFilterPlanJoin(wrapWithExistingSession, "lte", (l, r) => l.<=(r))
  }}

  test("testAsymmetricFilterPlanJoinGt") { not_Cluster {
    doTestAsymmetricFilterPlanJoin(wrapWithExtension, "gt", (l, r) => l.>(r))
  }}

  test("testAsymmetricFilterPlanJoinGtViaExistingSession") { onlyWithExtension {
    doTestAsymmetricFilterPlanJoin(wrapWithExistingSession, "gt", (l, r) => l.>(r))
  }}

  test("testAsymmetricFilterPlanJoinGte") { not_Cluster {
    doTestAsymmetricFilterPlanJoin(wrapWithExtension, "gte", (l, r) => l.>=(r))
  }}

  test("testAsymmetricFilterPlanJoinGteViaExistingSession") { onlyWithExtension {
    doTestAsymmetricFilterPlanJoin(wrapWithExistingSession, "gte", (l, r) => l.>=(r))
  }}

  val higher = 1314564453825188563L
  val lower = -6605018797301088250L

  val theuuid6Higher = SEqualTo("ahigher", higher)

  def doTestAsymmetricFilterPlanJoin(viaExtension: (SparkSession => Unit) => Unit, hint: String,
                                     joinOp: (Column, Column) => Column): Unit = when_not_disabled {
    doTestAsymmetricFilterPlan(viaJoinOnContext(joinOp), Seq(
      (s" '${theuuid + 6}' = acontext", theuuid6Higher, s"expr_rhs $hint"),
      (s" acontext = '${theuuid + 6}'", theuuid6Higher, s"expr_lhs $hint"),
      (s" '${theuuid + 6}' = acontext and bhigher > 0", theuuid6Higher, s"expr_rhs with further filter $hint"),
      (s" acontext = '${theuuid + 6}' and bhigher > 0", theuuid6Higher, s"expr_lhs with further filter $hint"),
      (s" acontext > '${theuuid + 6}' and bhigher > 0",
        SOr(SAnd(SEqualTo("ahigher",higher),
          SGreaterThan("alower",lower)),
          SGreaterThan("ahigher",higher)), s"expr_lhs gt with further filter $hint")
    ), true, viaExtension = viaExtension) }

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
  def verifyJoinPlanUUID(ds: DataFrame): Boolean = //  ds.queryExecution.optimizedPlan
    ClassicSparkTestUtils.getExecutedPlan(ds).map(_.logicalLink.collect {
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
    }.flatten).nonEmpty

  def doTestAsymmetricFilterPlan(withContextF: SparkSession => DataFrame, filters: Seq[(String, Filter, String)],
                                 joinTest: Boolean = false, viaExtension: (SparkSession => Unit) => Unit = wrapWithExtension,
                                 verifyJoinPlan: DataFrame => Boolean = verifyJoinPlanUUID
                                ): Unit = when_not_disabled {
    viaExtension { tsparkSession: SparkSession =>
      val withcontext = withContextF(tsparkSession)

      filters.foreach{ case (filter, expectedFilter, hint) =>
        val ds = withcontext.filter(filter)

        def assertWithPlan(condition: Boolean, hint: Any) = {
          if (!condition) {
            com.sparkutils.testing.TestUtils.debug(println(s"<---- filter was $filter"))
            // ds.explain(true)
          }
          assert(condition, hint)
        }

        val pushdowns = ClassicTestUtils.getPushDowns( ClassicSparkTestUtils.getExecutedPlan(ds).get )

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
  def verifyJoinPlanID(ds: DataFrame): Boolean = //  ds.queryExecution.optimizedPlan
    ClassicSparkTestUtils.getExecutedPlan(ds).map(_.logicalLink.collect {
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
    }.flatten).nonEmpty

  val theSixthIDString = "AbRr/ChS6QAAAAAMA/hChwAAAAY="
  val testI1= 286051723926044678L
  val theSeventhIDString = "AbRr/ChS6QAAAAAMA/hChwAAAAc="
  val threeLongIDString = "AAAAAwAAAAAAAAB7AAAAAAAAMEQAAAAC39vnuA=="

  def doAsymmetricFilterPlanCallIdsFields(generator: SparkSession => DataFrame, viaExtension: (SparkSession => Unit) => Unit = wrapWithExtension): Unit = when_not_disabled {
    doTestAsymmetricFilterPlan(generator, Seq(
      (s" '$theSixthIDString' = id", SEqualTo("i1",testI1), "expr_rhs"),
      (s" id = '$theSixthIDString'", SEqualTo("i1",testI1), "expr_lhs"),
      (s" '$theSixthIDString' = id and i1 > 286051723926044673L", SEqualTo("i1",testI1), "expr_rhs with further filter"),
      (s" id = '$theSixthIDString' and i1 > 286051723926044673L", SEqualTo("i1",testI1), "expr_lhs with further filter"),
      (s" id in ('$theSixthIDString', '$theSeventhIDString')", SIn("i1",Array(testI1, 286051723926044679L)), "with in")
    ), viaExtension = viaExtension, verifyJoinPlan = verifyJoinPlanID) }
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
    therows.toDS().selectExpr(s"base as ${prefix}base", s"i0 as ${prefix}i0", s"i1 as ${prefix}i1").write.mode("overwrite").format(format).save(outputDir + s"/${format}_${prefix}asymfilter")

    val reread = tsparkSession.read.format(format).load(outputDir + s"/${format}_${prefix}asymfilter")
    val withcontext = reread.selectExpr("*", select)
    withcontext
  }

  val idsWithContextFields = (prefix: String) => (tsparkSession: SparkSession) =>
    genBase64(s"id_base64(${prefix}base, ${prefix}i0, ${prefix}i1) as ${prefix}id", prefix, tsparkSession)

  test("testAsymmetricFilterPlanIdCallFields") { not_Cluster {
    doAsymmetricFilterPlanCallIdsFields( idsWithContextFields(""), wrapWithExtension)
  } }

  test("testAsymmetricFilterPlanIdCallFieldsViaExistingSession") { onlyWithExtension {
    doAsymmetricFilterPlanCallIdsFields( idsWithContextFields(""), wrapWithExistingSession)
  } }

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

  def doTestDifferentLengthsIdJoin(viaExtension: (SparkSession => Unit) => Unit, hint: String, generator: ((Column, Column) => Column) => SparkSession => DataFrame, joinOp: (Column, Column) => Column): Unit = when_not_disabled {
    // will trigger the IF clause and return false, so no records are found and, given no broken down part equals, no pushed down predicates either.
    try {doTestAsymmetricFilterPlan(generator(joinOp), Seq(
      (s" '$theSixthIDString' = aid", SEqualTo("ai1",testI1), s"expr_rhs $hint")
    ), true, viaExtension = viaExtension, verifyJoinPlan = verifyJoinPlanID)
    } catch {
      case t: Throwable if anyCauseHas(t, _.getMessage().indexOf(" different sizes - did not have re-written join") > -1)=> ()
    } }

  def doTestDifferentLengthsIdJoinAndFilter(viaExtension: (SparkSession => Unit) => Unit, hint: String, generator: ((Column, Column) => Column) => SparkSession => DataFrame): Unit = when_not_disabled {
    // will trigger the IF clause and return false, so no records are found and, given no broken down part equals, no pushed down predicates either.
    try {doTestAsymmetricFilterPlan(generator((l, r) => l.===(r)), Seq(
      (s" '$theSixthIDString' = aid", SEqualTo("ai1",testI1), s"expr_rhs $hint")
    ), true, viaExtension = viaExtension, verifyJoinPlan = verifyJoinPlanID)
    } catch {
      case t: Throwable if anyCauseHas(t, _.getMessage().indexOf(" different sizes - did not have re-written join") > -1)=> ()
    } }

  test("testAsymmetricFilterPlanIdJoinDifferentSizeStruct") { not_Cluster {
    doTestDifferentLengthsIdJoin(wrapWithExtension, "structs different sizes",  viaJoinIDStructsLarger, (l, r) => l.===(r) )
  }}

  test("testAsymmetricFilterPlanIdJoinDifferentSizeFields") { not_Cluster {
    doTestDifferentLengthsIdJoin(wrapWithExtension, "fields different sizes",  viaJoinIDFieldsLarger, (l, r) => l.===(r) )
  }}

  test("testAsymmetricFilterPlanIdJoinDifferentSizeMixed") { not_Cluster {
    doTestDifferentLengthsIdJoin(wrapWithExtension, "mixed different sizes",  viaJoinIDsMixedLarger, (l, r) => l.===(r) )
  }}

  test("testAsymmetricFilterPlanIdJoinDifferentSizeStructLT") { not_Cluster {
    doTestDifferentLengthsIdJoin(wrapWithExtension, "structs different sizes",  viaJoinIDStructsLarger, (l, r) => l.<(r) )
  }}

  test("testAsymmetricFilterPlanIdJoinDifferentSizeFieldsLT") { not_Cluster {
    doTestDifferentLengthsIdJoin(wrapWithExtension, "fields different sizes",  viaJoinIDFieldsLarger, (l, r) => l.<(r) )
  }}

  test("testAsymmetricFilterPlanIdJoinDifferentSizeMixedLT") { not_Cluster {
    doTestDifferentLengthsIdJoin(wrapWithExtension, "mixed different sizes",  viaJoinIDsMixedLarger, (l, r) => l.<(r) )
  }}

  test("testAsymmetricFilterPlanIdCallStructs") { not_Cluster {
    doAsymmetricFilterPlanCallIdsFields( idsWithContextStruct(""), wrapWithExtension)
  }}

  test("testAsymmetricFilterPlanIdCallStructsViaExistingSession") { onlyWithExtension {
    doAsymmetricFilterPlanCallIdsFields( idsWithContextStruct(""), wrapWithExistingSession)
  }}

  test("testDifferentLengthsId") { not_Cluster{
    // will trigger the IF clause and return false, so no records are found and, given no broken down part equals, no pushed down predicates either.
    try {
      doTestAsymmetricFilterPlan(idsWithContextStruct(""), Seq(
        (s" '$threeLongIDString' = id", SEqualTo("id",testI1), "expr_rhs")
      ), viaExtension = wrapWithExtension, verifyJoinPlan = verifyJoinPlanID)
    } catch {
      case t: Throwable if anyCauseHas(t, _.getMessage().indexOf("expr_rhs - did not have any pushed down filters") > -1)=> ()
    }
  }}

  def doTestAsymmetricFilterPlanJoinIDS(viaExtension: (SparkSession => Unit) => Unit, hint: String,
                                     joinOp: (Column, Column) => Column, generator: ((Column, Column) => Column) => SparkSession => DataFrame): Unit = when_not_disabled {
    doTestAsymmetricFilterPlan(generator(joinOp), Seq(
      (s" '$theSixthIDString' = aid", SEqualTo("ai1",testI1), s"expr_rhs $hint"),
      (s" aid = '$theSixthIDString'", SEqualTo("ai1",testI1), s"expr_lhs $hint"),
      (s" '$theSixthIDString' = aid and ai1 > 286051723926044673L", SEqualTo("ai1",testI1), s"expr_rhs with further filter $hint"),
      (s" aid = '$theSixthIDString' and ai1 > 286051723926044673L", SEqualTo("ai1",testI1), s"expr_lhs with further filter $hint"),
      (s" aid > '$theSixthIDString' and ai1 > 286051723926044673L",
        SOr(SAnd(SAnd(SEqualTo("abase", 28601340), SEqualTo("ai0", 2905640895816663052L)),
          SGreaterThan("ai1", 286051723926044678L)), SOr(SAnd(SEqualTo("abase", 28601340),
          SGreaterThan("ai0", 2905640895816663052L)), SGreaterThan("abase", 28601340))), s"expr_lhs gt with further filter $hint")
    ), true, viaExtension = viaExtension, verifyJoinPlan = verifyJoinPlanID) }

  test("testAsymmetricFilterPlanJoinFieldsEq") { not_Cluster {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension, "eq", (l, r) => l.===(r), viaJoinIDFields)
  }}
  test("testAsymmetricFilterPlanJoinStructEq") { not_Cluster {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension, "eq", (l, r) => l.===(r), viaJoinIDStructs)
  }}
  test("testAsymmetricFilterPlanJoinMixedEq") { not_Cluster {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension, "eq", (l, r) => l.===(r), viaJoinIDsMixed)
  }}

  test("testAsymmetricFilterPlanJoinFieldsEqn") { not_Cluster {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension, "eqn", (l, r) => l.<=>(r), viaJoinIDFields)
  }}
  test("testAsymmetricFilterPlanJoinStructEqn") { not_Cluster {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension, "eqn", (l, r) => l.<=>(r), viaJoinIDStructs)
  }}
  test("testAsymmetricFilterPlanJoinMixedEqn") { not_Cluster {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension, "eqn", (l, r) => l.<=>(r), viaJoinIDsMixed)
  }}

  test("testAsymmetricFilterPlanJoinFieldsLt") { not_Cluster {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension, "lt", (l, r) => l.<(r), viaJoinIDFields)
  }}
  test("testAsymmetricFilterPlanJoinStructLt") { not_Cluster {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension, "lt", (l, r) => l.<(r), viaJoinIDStructs)
  }}
  test("testAsymmetricFilterPlanJoinMixedLt") { not_Cluster {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension, "lt", (l, r) => l.<(r), viaJoinIDsMixed)
  }}

  test("testAsymmetricFilterPlanJoinFieldsLtEq") { not_Cluster {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension, "lte", (l, r) => l.<=(r), viaJoinIDFields)
  }}
  test("testAsymmetricFilterPlanJoinStructLtEq") { not_Cluster {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension, "lte", (l, r) => l.<=(r), viaJoinIDStructs)
  }}
  test("testAsymmetricFilterPlanJoinMixedLtEq") { not_Cluster {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension, "lte", (l, r) => l.<=(r), viaJoinIDsMixed)
  }}

  test("testAsymmetricFilterPlanJoinFieldsGt") { not_Cluster {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension, "gt", (l, r) => l.>(r), viaJoinIDFields)
  }}
  test("testAsymmetricFilterPlanJoinStructGt") { not_Cluster {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension, "gt", (l, r) => l.>(r), viaJoinIDStructs)
  }}
  test("testAsymmetricFilterPlanJoinMixedGt") { not_Cluster {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension, "gt", (l, r) => l.>(r), viaJoinIDsMixed)
  }}

  test("testAsymmetricFilterPlanJoinFieldsGtEq") { not_Cluster {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension, "gte", (l, r) => l.>=(r), viaJoinIDFields)
  }}
  test("testAsymmetricFilterPlanJoinStructGtEq") { not_Cluster {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension, "gte", (l, r) => l.>=(r), viaJoinIDStructs)
  }}
  test("testAsymmetricFilterPlanJoinMixedGtEq") { not_Cluster {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExtension, "gte", (l, r) => l.>=(r), viaJoinIDsMixed)
  }}


  test("testAsymmetricFilterPlanJoinFieldsEqViaExistingSession") { onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession, "eq", (l, r) => l.===(r), viaJoinIDFields)
  }}
  test("testAsymmetricFilterPlanJoinStructEqViaExistingSession") { onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession, "eq", (l, r) => l.===(r), viaJoinIDStructs)
  }}
  test("testAsymmetricFilterPlanJoinMixedEqViaExistingSession") { onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession, "eq", (l, r) => l.===(r), viaJoinIDsMixed)
  }}

  test("testAsymmetricFilterPlanJoinFieldsEqnViaExistingSession") { onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession, "eqn", (l, r) => l.<=>(r), viaJoinIDFields)
  }}
  test("testAsymmetricFilterPlanJoinStructEqnViaExistingSession") { onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession, "eqn", (l, r) => l.<=>(r), viaJoinIDStructs)
  }}
  test("testAsymmetricFilterPlanJoinMixedEqnViaExistingSession") { onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession, "eqn", (l, r) => l.<=>(r), viaJoinIDsMixed)
  }}

  test("testAsymmetricFilterPlanJoinFieldsLtViaExistingSession") { onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession, "lt", (l, r) => l.<(r), viaJoinIDFields)
  }}
  test("testAsymmetricFilterPlanJoinStructLtViaExistingSession") { onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession, "lt", (l, r) => l.<(r), viaJoinIDStructs)
  }}
  test("testAsymmetricFilterPlanJoinMixedLtViaExistingSession") { onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession, "lt", (l, r) => l.<(r), viaJoinIDsMixed)
  }}

  test("testAsymmetricFilterPlanJoinFieldsLtEqViaExistingSession") { onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession, "lte", (l, r) => l.<=(r), viaJoinIDFields)
  }}
  test("testAsymmetricFilterPlanJoinStructLtEqViaExistingSession") { onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession, "lte", (l, r) => l.<=(r), viaJoinIDStructs)
  }}
  test("testAsymmetricFilterPlanJoinMixedLtEqViaExistingSession") { onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession, "lte", (l, r) => l.<=(r), viaJoinIDsMixed)
  }}

  test("testAsymmetricFilterPlanJoinFieldsGtViaExistingSession") { onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession, "gt", (l, r) => l.>(r), viaJoinIDFields)
  }}
  test("testAsymmetricFilterPlanJoinStructGtViaExistingSession") { onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession, "gt", (l, r) => l.>(r), viaJoinIDStructs)
  }}
  test("testAsymmetricFilterPlanJoinMixedGtViaExistingSession") { onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession, "gt", (l, r) => l.>(r), viaJoinIDsMixed)
  }}

  test("testAsymmetricFilterPlanJoinFieldsGtEqViaExistingSession") { onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession, "gte", (l, r) => l.>=(r), viaJoinIDFields)
  }}
  test("testAsymmetricFilterPlanJoinStructGtEqViaExistingSession") { onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession, "gte", (l, r) => l.>=(r), viaJoinIDStructs)
  }}
  test("testAsymmetricFilterPlanJoinMixedGtEqViaExistingSession") { onlyWithExtension {
    doTestAsymmetricFilterPlanJoinIDS(wrapWithExistingSession, "gte", (l, r) => l.>=(r), viaJoinIDsMixed)
  }}
}

case class TestRow(lower: Long, higher: Long, asString: String)
case class TestID(base: Int, i0: Long, i1: Long)

class ExtensionParquetTest extends ExtensionTestBase {
  val format = "parquet"

  val shouldRun = true
}

class ExtensionDeltaTest extends ExtensionTestBase {
  val format = "delta"

  val shouldRun = true

  // test doesn't run in parquet due to some weird hive issue.
  test("testAsymmetricFilterEqSQL") { when_not_disabled { not_Cluster {
    wrapWithExtensionT(sparkSession => {
      val ds = uuidPairsWithContext("a")(sparkSession)
      val abspath = new File(ds.inputFiles.head).getParentFile.getPath.replaceAll("\\\\", "/")
      sparkSession.sql(s"drop table if exists testme")
      sparkSession.sql(s"create table testme using $format location '$abspath'")

      sparkSession.sql(s"create or replace view testfunctionview as select alower, ahigher, as_uuid(alower, ahigher) context from testme");
      val s = sparkSession
      val resdf = sparkSession.sql(s"select context from testfunctionview where context = '${theuuid + "6"}' limit 10")
      /*val res = resdf.as[String].collect()
      assert(res.length == 1)
      assert(res.head == (theuuid + "6"))
*/
      // verify push downs
      val pushdowns = ClassicTestUtils.getPushDowns( ClassicSparkTestUtils.getExecutedPlan(resdf).get )

      // with joins both sides should have pushdown for equals, but for gt,lt etc. it'll be one-sided for some, not for others
      assert(pushdowns.nonEmpty, s"did not have any pushed down filters")

      assert(pushdowns.contains(theuuid6Higher), s"did not have a pushdown with the correct predicates including $theuuid6Higher but $pushdowns")
    }, withHive = true)
  }}}

}
