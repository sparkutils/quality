---
tags:
   - basic
   - getting started
   - beginner
---

# Key SQL Functions to use in your Rules

## Expressions with constants
* passed() - the value representing a passed rule
* failed() - the value representing a failed rule
* soft_failed() - the value representing a failed rule which doesn't break the bank
* disabled_rule() - the value representing a rule which has been disabled and should be ignored

## Expressions which take expression parameters
* probability(x) - returns the probability (between 0.0 for a fail and 1.0 for pass) of a rule result
* pack_ints(lower, higher) - returns a Long with both the lower and higher int's packed in, used for id matching
* soft_fail( x ) - if the expression doesn't result in a Passed it returns softFailed() which does not trigger an overall failed() RuleSuite, this is ideal for when you want to flag a rule as passing a test you wish to query on later but do not care if it doesn't pass.  It can be treated as a "warn" or passed() expression.
* rule_suite_result_details( ruleSuiteResult ) - separates the RuleSuiteResult.overallResult from the rest of the structure should it be needed typically this is done via the addOverallResultsAndDetailsF
* rule_result(ruleSuiteResultColumn, packedRuleSuiteId, packedRuleSetId, packedRuleId) uses the packed long id's to retrieve the integer ruleResult (or ExpressionRunner result) or null if it can't be found.  
