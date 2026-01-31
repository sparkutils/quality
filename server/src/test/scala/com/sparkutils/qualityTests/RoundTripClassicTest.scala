package com.sparkutils.qualityTests

import com.sparkutils.qualityTests.util.{ClassicSharedTests, SharedConnectTests}

class RoundTripClassicTest extends SharedConnectTests with RoundTripTestBase {
  /**
   * Verify roundtripping of storage
   */
  test("ruleSuiteRoundTrippingToDF") {
    evalCodeGens {
      funNRewrites {
        doRuleSuiteRoundTrippingToDF()
      }
    }
  }

  /**
   * Verify roundtripping of storage
   */
  test("ruleEngineSuiteRoundTrippingToDF") {
    evalCodeGens {
      funNRewrites {
        doRuleEngineSuiteRoundTrippingToDF()
      }
    }
  }

  test("RuleDefaultProcessorRoundTrippingToDF") {
    evalCodeGens {
      funNRewrites {
        doRuleDefaultProcessorRoundTrippingToDF()
      }
    }
  }
}
