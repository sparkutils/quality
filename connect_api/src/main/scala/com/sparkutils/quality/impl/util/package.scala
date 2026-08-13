package com.sparkutils.quality.impl

import com.sparkutils.quality

package object util {

  @deprecated(message = "Provided for compatibility only, please migrate to the quality package versions", since = "0.2.0")
  object RuleModel {
    /**
     * Returned from RuleSuite loading
     */
    @deprecated(message = "Provided for compatibility only, please migrate to the quality package versions", since = "0.2.0")
    type RuleSuiteMap = quality.RuleModel.RuleSuiteMap
  }

  @deprecated(message = "Provided for compatibility only, please migrate to the quality package versions", since = "0.2.0")
  type RuleRow = quality.RuleRow

  @deprecated(message = "Provided for compatibility only, please migrate to the quality package versions", since = "0.2.0")
  type LambdaFunctionRow = quality.LambdaFunctionRow

  @deprecated(message = "Provided for compatibility only, please migrate to the quality package versions", since = "0.2.0")
  type OutputExpressionRow = quality.OutputExpressionRow
  @deprecated(message = "Provided for compatibility only, please migrate to the quality package versions", since = "0.2.0")
  type RuleSuiteRow = quality.RuleSuiteRow
  @deprecated(message = "Provided for compatibility only, please migrate to the quality package versions", since = "0.2.0")
  type SimpleField = quality.SimpleField
  @deprecated(message = "Provided for compatibility only, please migrate to the quality package versions", since = "0.2.0")
  type MetaRuleSetRow = quality.MetaRuleSetRow

  @deprecated(message = "Provided for compatibility only, please migrate to the quality package versions", since = "0.2.0")
  type RuleResultRow = quality.RuleResultRow
}
