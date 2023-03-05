---
tags:
   - model
---

## Rules

```plantuml title="Quality RuleSuite Class Model"

'!pragma layout smetana
!include <C4/C4_Context>

title Quality RuleSuite Class Model

VersionedId *-- Rule
VersionedId *-- LambdaFunction
LambdaFunction *-- Expression
Expression *-- RunOnPassProcessor
LambdaFunction *-- RunOnPassProcessor
VersionedId *-- RunOnPassProcessor
LogicRule *-- Rule
RunOnPassProcessor *-- Rule:When using QualityEngine
Rule *-- RuleSet
RuleSet *-- RuleSuite
VersionedId *-- RuleSet
VersionedId *-- RuleSuite
LambdaFunction *-- RuleSuite:Versioned RuleSuite or Global Lambdas
LambdaFunction -- LogicRule:LogicRules may use Lambdas within the same RuleSuite
Expression *-- LogicRule
abstract class Expression
note bottom of Expression
Spark Catalyst
Expression
end note
class VersionedId {
+int id
+int version
}
abstract class LogicRule {
+Expression expression
}
class LambdaFunction {
+string lambdaExpression
+string name
+VersionedId id
}
class Rule {
+LogicRule rule
+VersionedId version
+RunOnPassProcessor runOnPassProcessor
}
abstract class RunOnPassProcessor {
+Int salience
+Expression expression
}
note right of RunOnPassProcessor
Only used with
QualityEngine
end note
class RuleSet {
+VersionedId id
+Seq<Rule> rules
}
class RuleSuite {
+VersionedId id
+Seq<RuleSet> ruleSets
+Seq<LambdaFunction> lambdaFunctions
}
```

VersionedIDs are used throughout, changes to a Rule should imply a new Rule version, a new RuleSet version and a new RuleSuite version.

RunOnPassProcessor (output expressions) should only be provided when using the ruleEngineRunner and are treated, like Lambdas, as top level unique concepts.  You should organise using output expressions wherever possible as it's not only easier to conceptualise but it's also faster.  

## Rule Results
```plantuml title="Quality Rule and Engine Results Class Model"

'!pragma layout smetana
!include <C4/C4_Context>

title Quality Results Model

RuleResult <|-- Passed:Singleton
RuleResult <|-- Failed:Singleton
RuleResult <|-- SoftFailed:Singleton
RuleResult <|-- DisabledRule:Singleton
RuleResult <|-- Probability
RuleResult <|-- RuleResultWithProcessor
RuleResult *-- RuleSetResult
RuleSetResult *-- RuleSuiteResult
VersionedId *-- RuleSetResult
VersionedId *-- RuleSuiteResult
IdTriple *-- RuleEngineResult
RuleSuiteResult *-- RuleEngineResult
OutputExpression *-- RuleEngineResult
RuleSuiteResult *-- RuleEngineDebugResult
OutputExpression *-- SalientResult
SalientResult *-- RuleEngineDebugResult
VersionedId *-- IdTriple

class RuleResult {
}
class Passed {
}
class Failed {
}
class SoftFailed {
}
class DisabledRule {
}
class Probability {
+double percentage
}
class RuleResultWithProcessor {
+RuleResult ruleResult
+RunOnPassProcessor runOnPassProcessor
}
class VersionedId {
+int id
+int version
}

abstract class OutputExpression {
}

class IdTriple {
+VersionedId ruleSuiteId
+VersionedId ruleSetId
+VersionedId ruleId
}

class RuleEngineResult {
+RuleSuiteResult ruleSuiteResults
+IdTriple salientRule
+OutputExpression result
}

class SalientResult {
+int salience
+OutputExpression result
}

class RuleEngineDebugResult {
+RuleSuiteResult ruleSuiteResults
+IdTriple salientRule
+Array<SalientResult> result
}
note top of RuleEngineDebugResult
Only when using ruleEngineRunner with debugMode=true
end note
note right of RuleEngineDebugResult::salientRule
null when in debugMode=true
end note

class RuleSetResult {
+VersionedId id
+RuleResult overallResult
+Map<VersionedId, RuleResult> ruleResults
}
class RuleSuiteResult {
+VersionedId id
+RuleResult overallResult
+Map<VersionedId, RuleSetResult> ruleSetResults
}
```

* SoftFailed results do not cause the RuleSet or RuleSuite to fail
* DisabledRule results also do not cause the RuleSet or RuleSuite to fail but signal a rule has been disabled upstream
* Probability results with over 80 percent are deemed to have Passed, you may override this with the RuleSuite.withProbablePass function after creating the RuleSuite.

RuleResultWithProcessor is only used when using the ruleEngineRunner and is not returned in the column, rather the result of the expression is - shown above as call to "data".