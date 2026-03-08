package com.sparkutils.qualityTests

import com.sparkutils.quality.{Id, LambdaFunction, registerLambdaFunctions}
import com.sparkutils.qualityTests.util.{ClassicSharedTests, SharedConnectTests}
import com.sparkutils.testing.TestUtils
import org.apache.spark.sql.functions.lit

class UserLambdaFunctionClassicTest extends SharedConnectTests with UserLambdaFunctionTestBase {

  import org.apache.spark.sql.ShimUtils.expression

  def doTestPlaceHolderNullableOverrides(): Unit = {
    val resolve = TestUtils.resolveBuiltinOrTempFunction(sparkSession) _
    // as these cannot be tested as part of runtimes with aggregate bug resolve is used to directly test
    val actualDefaultCall = resolve("_", Seq(expression(lit("int")))).get
    assert(actualDefaultCall.nullable)
    val actualOverriddenCall = resolve("_", Seq(expression(lit("int")), expression(lit(false)))).get
    assert(!actualOverriddenCall.nullable)

    // this one is actually passed around as a lambda so it cannot be re-written.
    val plus = LambdaFunction("plus", "(a, b) -> a + b", Id(1, 2))
    // this isn't actually tested for false or true as the top level binding overrides it, but it's tested to prove coverage
    val test = LambdaFunction("plusTest", "(f, a) -> callFun(callFun(f, _('long', false), 1), a)", Id(3, 2))
    val test2 = LambdaFunction("plusTest2", "(f, a) -> callFun(callFun(f, _('long'), 1), a)", Id(3, 2))
    registerLambdaFunctions(Seq(plus, test, test2))

    var shouldBeNull = sparkSession.sql("select plusTest(plus(_(), _()), null)").head()
    assert(shouldBeNull.isNullAt(0))
    shouldBeNull = sparkSession.sql("select plusTest(plus(_(), _('int', false)), null)").head()
    assert(shouldBeNull.isNullAt(0))
    val control = sparkSession.sql("select plusTest(plus(_(), _()), 1L)").head()
    assert(!control.isNullAt(0))
    assert(control.get(0) == 2)
    val control2 = sparkSession.sql("select plusTest2(plus(_(), _()), 1L)").head()
    assert(!control2.isNullAt(0))
    assert(control2.get(0) == 2)
  }

  test("nullInParam") {
    evalCodeGensNoResolve {
      funNRewrites {
        doNullInParam()
      }
    }
  }

  test("lambdaRuleTest") {
    evalCodeGensNoResolve {
      funNRewrites {
        doLambdaRuleTest()
      }
    }
  }

  test("lambdaNoParamsRuleTest") {
    evalCodeGensNoResolve {
      funNRewrites {
        doLambdaNoParamsRuleTest()
      }
    }
  }

  test("lambdaMultiParamLengthExpandedTest") {
    evalCodeGensNoResolve {
      funNRewrites {
        doLambdaMultiParamLengthExpandedTest()
      }
    }
  }

  test("lambdaMultiParamLengthSelfReferenceTest") {
    evalCodeGensNoResolve {
      funNRewrites {
        doLambdaMultiParamLengthSelfReferenceTest()
      }
    }
  }

  test("lambdaMultiParamDupeLengthTest") {
    evalCodeGensNoResolve {
      funNRewrites {
        doLambdaMultiParamDupeLengthTest()
      }
    }
  }

  test("lambdaMissing0LengthTest") {
    evalCodeGensNoResolve {
      funNRewrites {
        doLambdaMissing0LengthTest()
      }
    }
  }

  test("nestedLambdaRuleTest") {
    evalCodeGensNoResolve {
      funNRewrites {
        doNestedLambdaRuleTest()
      }
    }
  }

  test("globalLambdasTest") {
    funNRewrites {
      doGlobalLambdasTest()
    }
  }

  /**
   * test's functions as params to lambdas, partial application cases are also
   * tested in the AggregatesTest - impl needs interpreted as the type of FunForward really isn't long
   */
  test("hofTest") {
    evalCodeGensNoResolve {
      funNRewrites {
        doHofTest()
      }
    }
  }

  test("deepPartialTest") {
    evalCodeGensNoResolve {
      funNRewrites {
        doDeepPartialTest()
      }
    }
  }

  test("returnLambdaTest") {
    evalCodeGensNoResolve {
      funNRewrites {
        doReturnLambdaTest()
      }
    }
  }

  test("testHOFLambdaDropin") {
    evalCodeGensNoResolve {
      funNRewrites {
        doHOFLambdaDropin()
      }
    }
  }

  /**
   * Although Dropin tests aggregate and combinations to prove overall behaviour, in light of the issue detected in DBRs
   * this tests all the other in built hofs.  Examples taken from the usage guide annotations
   */
  test("HOFDropins") {
    evalCodeGensNoResolve {
      funNRewrites {
        doTestHOFDropins()
      }
    }
  }

  test("HOFFunForwardDropin") {
    evalCodeGensNoResolve {
      funNRewrites {
        doTestHOFFunForwardDropin()
      }
    }
  }

  test("PlaceHolderNullableOverrides") {
    classicOnly {
      evalCodeGensNoResolve {
        funNRewrites {
          val resolve = TestUtils.resolveBuiltinOrTempFunction(sparkSession) _
          // as these cannot be tested as part of runtimes with aggregate bug resolve is used to directly test
          val actualDefaultCall = resolve("_", Seq(expression(lit("int")))).get
          assert(actualDefaultCall.nullable)
          val actualOverriddenCall = resolve("_", Seq(expression(lit("int")), expression(lit(false)))).get
          assert(!actualOverriddenCall.nullable)

          // this one is actually passed around as a lambda so it cannot be re-written.
          val plus = LambdaFunction("plus", "(a, b) -> a + b", Id(1, 2))
          // this isn't actually tested for false or true as the top level binding overrides it, but it's tested to prove coverage
          val test = LambdaFunction("plusTest", "(f, a) -> callFun(callFun(f, _('long', false), 1), a)", Id(3, 2))
          val test2 = LambdaFunction("plusTest2", "(f, a) -> callFun(callFun(f, _('long'), 1), a)", Id(3, 2))
          registerLambdaFunctions(Seq(plus, test, test2))

          var shouldBeNull = sparkSession.sql("select plusTest(plus(_(), _()), null)").head()
          assert(shouldBeNull.isNullAt(0))
          shouldBeNull = sparkSession.sql("select plusTest(plus(_(), _('int', false)), null)").head()
          assert(shouldBeNull.isNullAt(0))
          val control = sparkSession.sql("select plusTest(plus(_(), _()), 1L)").head()
          assert(!control.isNullAt(0))
          assert(control.get(0) == 2)
          val control2 = sparkSession.sql("select plusTest2(plus(_(), _()), 1L)").head()
          assert(!control2.isNullAt(0))
          assert(control2.get(0) == 2)
        }
      }
    }
  }

  test("CallFunForward") {
    evalCodeGensNoResolve {
      funNRewrites {
        doTestCallFunForward()
      }
    }
  }

}
