package com.sparkutils.qualityTests

import com.sparkutils.quality.Id
import com.sparkutils.quality.impl.YamlDecoder
import com.sparkutils.quality.impl.util.{Arrays, PrintCode}
import com.sparkutils.quality.types.ruleSuiteResultType
import com.sparkutils.qualityTests.util.{ClassicSharedTests, RowTools, SharedConnectTests}
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

  test("testPrintExpr") {
    classicOnly {
      funNRewrites {
        doTestPrint("Expression toStr is ->", "my message is", "my message is", "plus(1, 1, lambda", "printExpr")
      }
    }
  }

  // 2.4 doesn't support forceInterpreted so we can't test that it _doesn't_ compile, databricks is cluster based so we'll not be able to capture it without dumping to files
  test("testPrintCode") {
    classicOnly {
      not_Cluster {
        v3_2_and_above {

          val s = sparkSession

          // using eval we shouldn't get output
          forceInterpreted {
            {
              doTestPrint(null, "my message is", null, null, "printCode")
            }
          }

          // should generate output for code gen
          forceCodeGen {
            doTestPrint(PrintCode(expression(lit(""))).msg, "my message is", "my message is", "private int FunN_0(InternalRow i)", "printCode")
          }

          // irrespective of version we are outputting the lambda variables
          forceCodeGen {
            justfunNRewrite {
              doTestPrint(PrintCode(expression(lit(""))).msg, "my message is", "my message is", "LambdaVariable - b", "printCode")
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
    assert(2 == sparkSession.sql(s"select $expr(plus(1, 1)) as res").as[Long].head())

    def assertAdd() = if (addTest ne null) {
      assert(res.contains(addTest))
    }

    if (default ne null)
      assert(res.indexOf(default) == 0)
    else
      assert(res.isEmpty)
    assertAdd()

    assert(2 == sparkSession.sql(s"select $expr('$custom', plus(1, 1)) as res").as[Long].head())

    if (customTest ne null)
      assert(res.indexOf(customTest) == 0)
    else
      assert(res.isEmpty)

    assertAdd()
  }


  test("testRuleResult") {
    evalCodeGensNoResolve {
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
}
