package com.sparkutils.qualityTests

import com.sparkutils.quality.impl
import com.sparkutils.qualityTests.util.{ClassicSharedTests, SharedConnectTests}
import org.apache.spark.sql.catalyst.expressions.Literal

class RuleFolderClassicTest extends SharedConnectTests with RuleFolderTestBase {

  override def doTestFlattenResults(useSetSyntax: Boolean): Unit = funNRewrites {
    super.doTestFlattenResults(useSetSyntax)
  }

  override def doTestSimpleProductionRulesReplaceOutOfOrder(useSetSyntax: Boolean): Unit = funNRewrites {
    super.doTestSimpleProductionRulesReplaceOutOfOrder(useSetSyntax)
  }

  override def doTestSimpleProductionRulesReplaceCustomDDL(useSetSyntax: Boolean): Unit = funNRewrites {
    super.doTestSimpleProductionRulesReplaceCustomDDL(useSetSyntax)
  }

  override def doTestSimpleProductionRulesReplaceDebug(useSetSyntax: Boolean): Unit = funNRewrites {
    super.doTestSimpleProductionRulesReplaceDebug(useSetSyntax)
  }

  test("testSimpleProductionRules") {
    evalCodeGensNoResolve {
      funNRewrites {
        doTestSimpleProductionRules()
      }
    }
  }

  test("default processor"){
    evalCodeGensNoResolve {
      funNRewrites {
        doTestDefaultRules()
      }
    }
  }

  test("default processor via debug"){
    evalCodeGensNoResolve {
      funNRewrites {
        doTestDefaultRulesWithDebug()
      }
    }
  }


  test("testSetSyntaxButNoEqualTo") {
    classicOnly {
      val bad = impl.OutputExpression("set('lit')").expr
      assert(bad.children.head.getClass == Literal("lit").getClass)
    }
  }

  test("testSetSyntaxEqualToButNoAttribute") {
    classicOnly {
      val bad = impl.OutputExpression("set( 1 = 'lit' )").expr
      assert(bad.children.head.children.head.getClass == Literal("lit").getClass)
    }
  }
}
