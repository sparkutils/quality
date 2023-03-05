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
* softFailed() - the value representing a failed rule which doesn't break the bank
* disabledRule() - the value representing a rule which has been disabled and should be ignored

## Expressions which take expression parameters
* probability(x) - returns the probability (between 0.0 for a fail and 1.0 for pass) of a rule result
* packInts(lower, higher) - returns a Long with both the lower and higher int's packed in, used for id matching
* softFail( x ) - if the expression doesn't result in a Passed it returns softFailed() which does not trigger an overall failed() RuleSuite, this is ideal for when you want to flag a rule as passing a test you wish to query on later but do not care if it doesn't pass.  It can be treated as a "warn" or passed() expression.
* ruleSuiteResultDetails( ruleSuiteResult ) - separates the RuleSuiteResult.overallResult from the rest of the structure should it be needed typically this is done via the addOverallResultsAndDetailsF