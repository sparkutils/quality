package com.sparkutils.qualityTests

import com.sparkutils.quality._
import com.sparkutils.quality.impl.RuleEngineRunner
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataType
import org.junit.Test
import org.scalatest.FunSuite

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

/**
 * Primarily to prove joins with relation's work with resolveWith across versions.
 * Secondary usage is to prove a simple join (vs maplookup approach) can work with resolveWith for lookups
 */
class JoinValidationTest extends FunSuite with RowTools with TestUtils {

  val testData=Seq(
    TestOn("edt", "4201", 40),
    TestOn("otc", "5201", 40),
    TestOn("fi", "4251", 50),
    TestOn("fx", "4206", 90),
    TestOn("fxotc", "4201", 40),
    TestOn("eqotc", "4201", 60)
  )

  val testSource=Seq(
    TestOn("edt", "4200", 40),
    TestOn("otc", "5201", 40),
    TestOn("fi", "4250", 50),
    TestOn("fx", "4206", 90),
    TestOn("fxotc", "4200", 40),
    TestOn("eqotc", "4201", 60)
  )

  def irules(expressionRules: Seq[ExpressionRule], compileEvals: Boolean = true, transformRuleSuite: RuleSuite => RuleSuite = identity) = {
    val rules =
      for { (exp, idOffset) <- expressionRules.zipWithIndex }
        yield Rule(Id(100 * idOffset, 1), exp)

    val rsId = Id(1, 1)
    val ruleSuite = RuleSuite(rsId, Seq(
      RuleSet(Id(50, 1), rules
      )))

    (dataFrame: DataFrame) =>
      ruleRunner(transformRuleSuite(ruleSuite),
        resolveWith = if (doResolve.get()) Some(dataFrame) else None, compileEvals = compileEvals)
  }

  val expected = Seq.fill(3)(Seq(Failed, Passed)).flatten

  @Test
  def testViaRelation(): Unit = evalCodeGensNoResolve {
    val rer = irules(
      Seq(ExpressionRule("testSource.account = testLookups.account")
      )
    )

    val (testDataDF, testSourceDataDF) = {
      import sparkSession.implicits._
      (testData.toDF(), testSource.toDF())
    }
    testDataDF.createOrReplaceTempView("testLookups")
    testSourceDataDF.createOrReplaceTempView("testSource")

    import com.sparkutils.quality.implicits._

    val testMeDF = sparkSession.sql(
      """select * from testSource
        left outer join testLookups on testSource.product = testLookups.product""")

    val outdf = testMeDF.withColumn("together", rer(testMeDF))
    //outdf.show

    val res = outdf.select("together.*").as[RuleSuiteResult].collect()
    // res.foreach(println)

    assert(expected == res.map(_.overallResult).toSeq)
  }

  @Test
  def testWithRenames(): Unit = evalCodeGensNoResolve {
    val rer = irules(
      Seq(ExpressionRule("account = testLookupsAccount")
      )
    )

    val (testDataDF, testSourceDataDF) = {
      import sparkSession.implicits._
      (testData.toDF(), testSource.toDF())
    }
    testDataDF.createOrReplaceTempView("testLookups")
    testSourceDataDF.createOrReplaceTempView("testSource")

    import com.sparkutils.quality.implicits._

    val testMeDF = sparkSession.sql(
      """select testSource.*, testLookups.account as testLookupsAccount from testSource
        left outer join testLookups on testSource.product = testLookups.product""")

    val outdf = testMeDF.withColumn("together", rer(testMeDF))
    //outdf.show

    val res = outdf.select("together.*").as[RuleSuiteResult].collect()
    //res.foreach(println)

    assert(expected == res.map(_.overallResult).toSeq)
  }

}
