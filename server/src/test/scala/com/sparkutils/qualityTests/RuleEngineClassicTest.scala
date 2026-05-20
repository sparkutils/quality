package com.sparkutils.qualityTests

import com.sparkutils.quality._
import com.sparkutils.quality.impl.{OverallResult, RuleEngineRunner}
import com.sparkutils.quality.impl.extension.{FunNRewrite, ZeroCodeGenWrap}
import com.sparkutils.qualityTests.util.SharedConnectTests
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.ShimUtils.expression
import org.apache.spark.sql.functions.col

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

class RuleEngineClassicTest extends SharedConnectTests with RuleEngineTestBase {

  override def doTestProbabilityRules(overallResult: OverallResult): Unit = funNRewrites {
    super.doTestProbabilityRules(overallResult)
  }

  test("testSimpleProductionRules Classic") {
    classicOnly {
      evalCodeGensNoResolve {
        testPlan(FunNRewrite, disable = _ == 32) {
          doSimpleProductionRules()
        }
      }
    }
  }

  test("testFlattenResults") {
    evalCodeGensNoResolve {
      funNRewrites {
        doTestFlattenResults()
      }
    }
  }

  test("testSalience") {
    evalCodeGensNoResolve {
      funNRewrites {
        doTestSalience()
      }
    }
  }

  test("testDebug") {
    evalCodeGens {
      funNRewrites {
        doTestDebug()
      }
    }
  }

  test("testHugeAmountOfRulesSOE Classic") {
    classicOnly {
      evalCodeGensNoResolve {
        funNRewrites {
          val rer = irules(
            Seq.fill(4000)(ExpressionRule(1 to 50 map ((i: Int) => s"(product = 'edt' and subcode = ${40 + i})") mkString " or "),
              RunOnPassProcessor(1000, Id(3010, 1),
                OutputExpression("array(account_row('from', account), account_row('to', 'other_account1'))"))), compileEvals = false
          )(null.asInstanceOf[DataFrame]) // the df is irrelevant as we are NoResolving

          val rs = expression(rer) match {
            case r: RuleEngineRunner => r.ruleSuite
            case ZeroCodeGenWrap(_, r: RuleEngineRunner) => r.ruleSuite
          }
          val ds = toDS(rs)

          val so = toOutputExpressionDS(rs)

          val ruleMapWithoutOE = readRulesFromDF(ds.toDF(),
            col("ruleSuiteId"),
            col("ruleSuiteVersion"),
            col("ruleSetId"),
            col("ruleSetVersion"),
            col("ruleId"),
            col("ruleVersion"),
            col("ruleExpr"),
            col("ruleEngineSalience"),
            col("ruleEngineId"),
            col("ruleEngineVersion")
          )
          val outputExpressions = readOutputExpressionsFromDF(so.toDF(),
            col("ruleExpr"),
            col("functionId"),
            col("functionVersion"),
            col("ruleSuiteId"),
            col("ruleSuiteVersion")
          )

          val (ruleMap, missing) = integrateOutputExpressions(ruleMapWithoutOE, outputExpressions, Some(Id(-1, -1))) // non-existent but shouldn't throw key not found exception

          // attempt to serialise, it if works that's enough to pass as throwing an SOE is the problem
          val rerer = ruleEngineRunner(ruleMap.head._2)

          val bos = new ByteArrayOutputStream()
          val os = new ObjectOutputStream(bos)
          os.writeObject(expression(rerer))
          val bytes = bos.toByteArray()
        }
      }
    }
  }
// case class Compilation_110(STARTER: String, STARTER_FLAG: String, I1: String, I2: String, I3: String, I4: String)

  val compilationTestData = Seq(
    Compilation_110("a", "af", Some("a"), "A", "b", None),
    Compilation_110("a", "af", None, "A", "b", Some("c")),
    Compilation_110("a", "af", Some("a"), "A", "b", None),
    Compilation_110("a", "af", None, "A", "b", Some("c")),
    Compilation_110("a", "af", Some("a"), "A", "b", None)
  )

  // Using compileEvals = true, forceTriggerEval = true will cause compilation and runtime issues when nesting
  // and it being wholestage relevant (needs files)
  test("Deeply nested projection causes compilation issue") { evalCodeGensNoResolve {
    import com.sparkutils.quality._

    val spark = sparkSession
    import spark.implicits._

    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._

    val rule1 = Rule(Id(100, 2), ExpressionRule("I1 IS NOT NULL"), RunOnPassProcessor(1000, Id(1040,1),
      OutputExpression("array(STARTER, 'EX_1')")))
    val rule2 = Rule(Id(101, 1), ExpressionRule("I1 IS  NULL "), RunOnPassProcessor(1000, Id(1040,1),
      OutputExpression("array(STARTER_FLAG, 'EX_2')")))
    val ruleSuite = RuleSuite(Id(22, 1034), Seq(RuleSet(Id(33, 345), Seq(rule1,rule2))))
    val schema=DataType.fromDDL("ARRAY<STRING>")
    val rer :org.apache.spark.sql.Column = ruleEngineRunner(ruleSuite,Some(schema))//, compileEvals = true, forceTriggerEval = true)

    val fname = outputDir + "/compilation_110_testData"

    compilationTestData.toDS().write.mode("overwrite").parquet(fname)

    val rulesDf = //compilationTestData.toDS()
      spark.read
        .parquet(fname)

    val exprString = s"together.result as result"
    val oneOutDf = rulesDf.select(col("*"), rer.alias("together")).select(col("*"), expr(exprString)).
      withColumn("TEMP_O_1",col("result").getItem(0)).withColumn("RULE_ID",col("result").getItem(1)).
      drop("together","result")
    oneOutDf.head() // should work

    val rule3 = Rule(Id(100, 2), ExpressionRule("I2 != 'A' AND TEMP_O_1 IS NOT NULL "),
      RunOnPassProcessor(1000, Id(1040,1),OutputExpression("array(TEMP_O_1, 'a', 'EX_3')")))
    val rule4 = Rule(Id(101, 2), ExpressionRule("I2 = 'A' AND I3 IN ('a', 'b') AND I4 IS NOT NULL"),
      RunOnPassProcessor(1000, Id(1040,1),OutputExpression("array(I4, 'b', 'EX_4')")))
    val ruleSuite2 = RuleSuite(Id(22, 1034), Seq(RuleSet(Id(33, 345), Seq(rule3,rule4))))
    val schema2=DataType.fromDDL("ARRAY<STRING>")
    val rer2 :org.apache.spark.sql.Column = ruleEngineRunner(ruleSuite2,Some(schema2))//, compileEvals = true, forceTriggerEval = true)

    val twoOutDf = oneOutDf.select(col("*"), rer2.alias("together")).select(col("*"), expr(exprString)).
      withColumn("TEMP_O_2",col("result").getItem(0)).withColumn("REASON_CODE",col("result").getItem(1)).
      withColumn("RULE_ID",col("result").getItem(2))
      .drop("together","result")
    twoOutDf.head() // should work

    val rule5 = Rule(Id(100, 2),
      ExpressionRule("REASON_CODE IN ('a', 'b')  AND I2 = 'A' AND I3 IN ('a', 'b', 'c', 'd') AND TEMP_O_2 = 'E'"),
      RunOnPassProcessor(1000, Id(1040,1),OutputExpression("array('N', 'EX_6')")))
    val rule6 = Rule(Id(101, 2), ExpressionRule("TEMP_O_2 ='UNKNOWN' and I2 = 'A' AND I3 IN ('a', 'b', 'c', 'd')  "),
      RunOnPassProcessor(1000, Id(1040,1),OutputExpression("array('N', 'EX_7')")))
    val rule7 = Rule(Id(102, 2), ExpressionRule("TEMP_O_2 ='UNKNOWN'"), RunOnPassProcessor(1000, Id(1040,1),
      OutputExpression("array('R', 'EX_8')")))

    val ruleSuite3 = RuleSuite(Id(22, 1034), Seq(RuleSet(Id(33, 345), Seq(rule5,rule6,rule7))))
    val schema3=DataType.fromDDL("ARRAY<STRING>")
    val rer3 :org.apache.spark.sql.Column = ruleEngineRunner(ruleSuite3,Some(schema3))//, compileEvals = true, forceTriggerEval = true)

    val threeOutDf = twoOutDf.select(col("*"), rer3.alias("together")).select(col("*"), expr(exprString)).
      withColumn("TEMP_O_3",col("result").getItem(0)).withColumn("RULE_ID",col("result").getItem(1))
      .drop("together","result")
    //threeOutDf.cache()

    // with .cache these work, without they don’t on databricks
    //threeOutDf.selectExpr("*","TEMP_O_2 IS not NULL as test").filter($"test" === lit("false")).show()
    //display(threeOutDf.filter("TEMP_O_2 IS NULL"))
    threeOutDf.filter("TEMP_O_3 IS NULL").head() // doesn't work on 14.3
  } } // 22 times in CodeGenerator cook before the right code is there

  // override def loggingLevel: String = "DEBUG"
}

case class Compilation_110(STARTER: String, STARTER_FLAG: String, I1: Option[String], I2: String, I3: String, I4: Option[String])