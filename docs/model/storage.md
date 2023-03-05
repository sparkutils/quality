---
tags:
   - model
---

# Storage Model
Nested columns, with nested columns, this lets you use Spark SQL to do filters and have predicate pushdown.  Sample filter:

```scala
df.select(expr("filter(map_values(DataQuality.ruleSetResults), 
  ruleSet -> size(filter(map_values(ruleSet.ruleResults), 
  result ->  probability(result) > 0.3 )) > 0)").as("filtered"))
```

actual type:

```typescript
struct<id: LongType, overallResult: IntegerType, 
  ruleSetResults: map<LongType, 
    struct<overallResult: IntegerType, 
	  ruleResults: map<LongType, IntegerType>>>>
```

Alternatively when creating with addOverallResultsAndDetails you have the 

```typescript
overallResult: IntegerType
```

moved to the top level, leaving

```typescript
details: struct<id: LongType, 
  ruleSetResults: map<LongType, 
    struct<overallResult: IntegerType, 
	  ruleResults: map<LongType, IntegerType>>>>
```

## Where have all the VersionIds and RuleResults gone?

In order to optimise storage and marshalling the VersionId parts are packed into a single LongType.  RuleResults are similarly encoded into an IntegerType:

* Failed => FailedInt // 0
* SoftFailed => SoftFailedInt // -1
* Disabled => DisabledInt // -2
* Passed => PassedInt // 100000
* Probability(percentage) => (percentage * PassedInt).toInt

When the developer wishes to retrieve the objects they may use the encoders directly:

```scala
// frameless is used to encode
import frameless._
// imports the encoders for RuleSuiteResult
import com.sparkutils.quality.implicits._
// derive an encoder for the pair with a user type and the RuleSuiteResult for a given row
implicit val enc = TypedExpressionEncoder[(TestIdLeft, RuleSuiteResult)]
// select the fields needed for the user type and the DataQuality result (or details with RuleResult, RuleSuiteResultDetails for seperate overall results and details)
val ds = df.selectExpr("named_struct('left_lower', `1`, 'left_higher', `2`)","DataQuality").as[(TestIdLeft, RuleSuiteResult)]
```

the developer can then interegate the data quality results alongside their relevant data.