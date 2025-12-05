package com.sparkutils.qualityTests.classicOnly

import com.sparkutils.quality.impl.RuleEngineRunner
import com.sparkutils.quality.impl.extension.FunNRewrite
import com.sparkutils.quality._
import com.sparkutils.qualityTests.RuleEngineTestBase
import com.sparkutils.qualityTests.util.ClassicSharedTests
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.ShimUtils.expression
import org.apache.spark.sql.functions.col

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

class ClassicRuleEngineTest extends ClassicSharedTests with RuleEngineTestBase {

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

          val rs = expression(rer).asInstanceOf[RuleEngineRunner].ruleSuite
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

}
