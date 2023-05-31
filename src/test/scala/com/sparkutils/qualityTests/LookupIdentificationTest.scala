package com.sparkutils.qualityTests

import com.sparkutils.quality.utils.{BloomLookupType, MapLookupType}
import org.junit.Test
import org.scalatest.FunSuite

class LookupIdentificationTest extends FunSuite with TestUtils {

  @Test
  def mapLookuplambdaTest = doSimpleLambdaTest("mapLookup", MapLookupType(_), asis)

  @Test
  def mapContainslambdaTest = doSimpleLambdaTest("mapContains", MapLookupType(_), asis)

  @Test
  def probabilityInlambdaTest = doSimpleLambdaTest("probabilityIn", BloomLookupType(_), flipped)

  @Test
  def saferRowIdlambdaTest = doSimpleLambdaTest("saferRowId", BloomLookupType(_), flipped)

  var asis = (arg1: String, arg2: String) => s"$arg1, $arg2"
  var flipped = (arg1: String, arg2: String) => s"$arg2, $arg1"

  def doSimpleLambdaTest[A](lookupF: String, lookupType: String => A, argMaker: (String, String) => String): Unit = evalCodeGensNoResolve {
    import com.sparkutils.quality._
    registerQualityFunctions()
    registerLongPairFunction(Map.empty)

    val ruleSuite =  RuleSuite(Id(1,1), Seq.empty, Seq(
      LambdaFunction("rule1", s"$lookupF(${argMaker("'ccyRate'", "ccy")})", Id(2,3)),
      LambdaFunction("rule2", s"$lookupF(${argMaker("'funky'", "ccy")}) || IF(1 > 2, $lookupF(${argMaker("'funky2'", "ccy")}), $lookupF(${argMaker("'funky3'", "ccy")})) || $lookupF(${argMaker("'funkyFuncInFunc'", s" $lookupF(${argMaker("'funkyX'", "ccy")})")})", Id(20,3)),
      LambdaFunction("rule3", s"$lookupF(${argMaker("col1", "ccy")})", Id(40,3)),
      LambdaFunction("rule4", s"$lookupF(${argMaker("col2", "ccy")})", Id(60,3)),
      LambdaFunction("rule3", s"$lookupF(${argMaker("col1", "ccy")}) || $lookupF(${argMaker("'other'", "ccy")})", Id(80,3))
    ))

    val res = identifyLookups(ruleSuite)
    assert(res.lambdaResults.lookupExpressions == Set(Id(40,3), Id(60,3), Id(80,3)))
    val expected = Map(Id(2,3) -> Set(lookupType("ccyRate")),
      Id(20,3) -> Set(lookupType("funky"), lookupType("funky2"), lookupType("funky3"), lookupType("funkyFuncInFunc"),
        lookupType("funkyX")), Id(80,3) -> Set(lookupType("other")))
    assert(res.lambdaResults.lookupConstants == expected)
  }

  @Test
  def testMixed: Unit = evalCodeGensNoResolve {
    import com.sparkutils.quality._
    registerQualityFunctions()

    val ruleSuite =  RuleSuite(Id(1,1), Seq.empty, Seq(
      LambdaFunction("rule1", s"probabilityIn(ccy, 'ccyRate') || mapLookup('ccyRate', ccy)", Id(2,3))
    ))

    val res = identifyLookups(ruleSuite)
//    assert(res.lambdaResults.lookupExpressions == Set(Id(40,3), Id(60,3), Id(80,3)))
    val expected = Map(Id(2,3) -> Set(MapLookupType("ccyRate"), BloomLookupType("ccyRate")) )
    assert(res.lambdaResults.lookupConstants == expected)

  }


}
