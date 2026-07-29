package com.sparkutils.qualityTests

import com.sparkutils.quality.{ExpressionRule, Failed, Id, IgnoredRule, Passed, Probability, Rule, RuleSet, RuleSuite}
import com.sparkutils.quality.impl.YamlDecoder
import com.sparkutils.quality.impl.util.{Arrays, PrintCode}
import com.sparkutils.quality.impl.types.ruleSuiteResultType
import com.sparkutils.qualityTests.util.{ClassicSharedTests, RowTools, SharedConnectTests}
import com.sparkutils.testing.SparkVersions
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.ShimUtils.expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.IntegerType
import org.scalatest.Matchers

class BaseFunctionalityClassicTest extends SharedConnectTests with RowTools with BaseFunctionalityShared with Matchers {

  test("mapArrays") {
    val ar = ArrayData.toArrayData(Seq(0,1,2,3,4)) // Force GenericArrayData instead of UnsafeArrayData
    val nar = Arrays.mapArray(ar, IntegerType, _.asInstanceOf[Integer] + 1)
    assert((0 until 5).forall(i => nar(i) == i + 1))

    // verify with simple toArray
    val nar2 = Arrays.toArray(ar, IntegerType)
    assert((0 until 5).forall(i => nar2(i) == i))
  }

  def v4_1_and_below(thunk: => Unit) =
    if (SparkVersions.sparkVersion == "4.2") {}
    else thunk

  test("testPrintExpr") {
    v4_1_and_below {
      classicOnly {
        funNRewrites {
          doTestPrint("Expression toStr is ->", "my message is", "my message is", "plus(1, 1, lambda", "printExpr")
        }
      }
    }
  }

  // 2.4 doesn't support forceInterpreted so we can't test that it _doesn't_ compile, databricks is cluster based so we'll not be able to capture it without dumping to files
  test("testPrintCode") {
    v4_1_and_below {
    classicOnly {
      not_Cluster {
        v3_2_and_above {

          val s = sparkSession

          // using eval we shouldn't get output
          forceInterpreted {  {
            doTestPrint(null, "my message is", null, null, "printCode")
          } }

          // should generate output for code gen
          forceCodeGen {
            doTestPrint(PrintCode(expression(lit(""))).msg, "my message is", "my message is", "private int FunN_0(InternalRow i)", "printCode")
            // irrespective of version we are outputting the lambda variables
            doTestPrint(PrintCode(expression(lit(""))).msg, "my message is", "my message is", "LambdaVariable - b", "printCode")
          }

          // rewrite can be Math or +
          forceCodeGen {
            justfunNRewrite {
              if (sparkVersionNumericMajor >= 40) {
                doTestPrint(PrintCode(expression(lit(""))).msg, "my message is", "my message is", "MathUtils.addExact", "printCode")
              } else {
                doTestPrint(PrintCode(expression(lit(""))).msg, "my message is", "my message is", "1 + 1", "printCode")
              }
            }
          }
        }
      }
    }
    }
  }

  def doTestPrint(default: String, custom: String, customTest: String, addTest: String, expr: String): Unit = {
    import com.sparkutils.quality._
    val s = sparkSession
    import s.implicits._
    val plus = LambdaFunction("plus", "(a, b) -> a + b", Id(3, 2)) // force compile with codegen
    registerLambdaFunctions(Seq(plus))
    Holder.res = ""
    classicFunctions.registerQualityFunctions(writer = {
      Holder.res = _
    })

    import Holder.res
    sparkSession.sql(s"select $expr(plus(1, 1)) as res").as[Long].head() shouldBe 2

    def assertAdd() = if (addTest ne null) {
      res should include(addTest)
    }

    if (default ne null)
      res should startWith(default)
    else
      assert(res.isEmpty)
    assertAdd()

    assert(2 == sparkSession.sql(s"select $expr('$custom', plus(1, 1)) as res").as[Long].head())

    if (customTest ne null)
      res should startWith(customTest)
    else
      assert(res.isEmpty)

    assertAdd()
  }


  test("testRuleResult") {
    forceInterpreted {
      funNRewrites {
        doTestRuleResult()
      }
    }
  }

  test("testRuleResultDetails") {
    evalCodeGensNoResolve {
      funNRewrites {
        doTestRuleResultDetails()
      }
    }
  }

  test("testExpressionsWithAggregate") {
    evalCodeGensNoResolve {
      funNRewrites {
        val res = doTestExpressionsWithAggregate()

        val yaml = YamlDecoder.yaml

        val obj = yaml.load[Long](res.ruleSetResults(Id(20,1))(Id(30,3)).ruleResult);
        assert(obj == 499500L)
      }
    }
  }

  test("Resolve should work correctly") {
    val rules = genRules(27, 27)

    val toWrite = 1 // writeRows

    var df: DataFrame = null
    classicOnly {
      evalCodeGens {
        df = taddDataQuality(dataFrameLong(toWrite, 27, ruleSuiteResultType, null), rules)
      }
    }
    connectOnly {
      try {
        doWithResolve {
          df = taddDataQuality(dataFrameLong(toWrite, 27, ruleSuiteResultType, null), rules)
        }
        fail("resolveWith isn't possible with connect so this should have thrown")
      } catch {
        case t: Throwable => t.getMessage.contains("resolveWith is being used with Connect, this is not a valid combination") shouldBe true
      }
    }
  }

  // retested for compilation
  test("compilation any test") {
    evalCodeGensNoResolve {
      resultChecker(
        rs = RuleSuite(Id(10, 2), Seq(RuleSet(Id(20, 1), Seq(
          Rule(Id(30, 3), ExpressionRule("'ignored'")),
          Rule(Id(31, 3), ExpressionRule("ignored_rule()")),
          Rule(Id(32, 3), ExpressionRule("-3")),
          Rule(Id(34, 3), ExpressionRule("cast(-3.0 as double)")),
          Rule(Id(35, 3), ExpressionRule("-3.0")),
          Rule(Id(36, 3), ExpressionRule("null")),
          Rule(Id(37, 3), ExpressionRule(s"id * $resultCheckerCodeGenSize")), // stop constant folding the output away to force codegen
        )))), (Failed, Failed), Seq(IgnoredRule, IgnoredRule, IgnoredRule, IgnoredRule, Probability(-3.0), Failed), _.toSeq.dropRight(1))
    }
  }

}
