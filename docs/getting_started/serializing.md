---
tags:
   - basic
   - getting started
   - beginner
---

## Reading & Writing RuleSuites

Typically you'd save the RuleSuite in configuration tables within a Database or Delta or some other easy to edit store.

Saving:

```scala
// The lambda functions from the RuleSuite
val lambdaDF = toLambdaDS(rules)
lambdaDF.write .....

// The rest of the rules
val ruleDF = toRuleSuiteDF(rules)
ruleDF.write .....
``` 

The field names used follow the convention of the default Product Encoder but can be renamed as desired.

Similarly, reading the rules can be as simple as:

```{.scala #rawread}
val rereadWithoutLambdas = readRulesFromDF(ruleDF,
    col("ruleSuiteId"),
    col("ruleSuiteVersion"),
    col("ruleSetId"),
    col("ruleSetVersion"),
    col("ruleId"),
    col("ruleVersion"),
    col("ruleExpr")
  )

val reReadLambdas = readLambdasFromDF(lambdaDF.toDF(),
    col("name"),
    col("ruleExpr"),
    col("functionId"),
    col("functionVersion"),
    col("ruleSuiteId"),
    col("ruleSuiteVersion")
  )

val reReadRuleSuite = integrateLambdas(rereadWithoutLambdas, reReadLambdas)

```

The column names used during reading are not assumed and must be specified.

## Versioned rule datasets

The user is completely free to chose their own version management approach, but the design is aimed at immutability and evidencing.

To make things easy a simple scheme with library functions in the simpleVersioning package are provided:

1. Rules can be added to rulesets (or indeed new rulesets) with just a single row within the input DF, this must increase the RuleSet _AND_ RuleSuites version:

    ruleSuiteId|ruleSuiteVersion|ruleSetId|ruleSetVersion|ruleId|ruleVersion|ruleExpr
    ----|----|----|----|----|----|----
    1 | 1 | 1 | 1 | 1 | 1 | `#!sql /* existing rule rows */ true()` 
    1 | **2** | 1 | **2** | **2** | 1 | `#!sql /* new rule */ failed() `

3. Similarly, you can change a rule by adding a new row which increments the Rule Id's, RuleSet _AND_ RuleSuites versions:

    ruleSuiteId|ruleSuiteVersion|ruleSetId|ruleSetVersion|ruleId|ruleVersion|ruleExpr
    ----|----|----|----|----|----|----
    1 | 1 | 1 | 1 | 1 | 1 | `#!sql /* existing rule row */ true()`
    1 | **2** | 1 | **2** | 1 | **2** | `#!sql /* new version of the above rule */ failed()`

5. To delete a rule you can either use disabled() to flag the rule is inactivated or DELETED to flag the rule to be removed from a RuleSet, as before each version must be incremented:

    ruleSuiteId|ruleSuiteVersion|ruleSetId|ruleSetVersion|ruleId|ruleVersion|ruleExpr 
    ----|----|----|----|----|----|----
    1 | 1 | 1 | 1 | 1 | 1 | `#!sql /* existing rule row */ true()` 
    1 | **2** | 1 | **2** | 1 | **2** | **DELETED**

7. OutputExpressions may be re-used with different versions (be it for QualityRules or QualityFolder), each rule row that needs to use a later OutputExpression must increment all of it's Id versions.  You may are advised to use lambdas to soften the impact:

    ruleSuiteId|ruleSuiteVersion|ruleSetId|ruleSetVersion|ruleId|ruleVersion|ruleExpr|ruleEngineSalience|ruleEngineId|ruleEngineVersion
    ----|----|----|----|----|----|----|----|----|----
    1 | 1 | 1 | 1 | 1 | 1 | `#!sql true()`| 60 | 100 | 1
    1 | **2** | 1 | **2** | 1 | **2** | `#!sql true()` | 60 | 100 | **2**

9. Lambda Expressions for a RuleSuite simply take the latest version for a given lambda id.  If you want to delete a lambda (for example you have used a name that is now an official Spark sql function) you can add a DELETED row for a given RuleSuite with a higher version.

    ruleSuiteId|ruleSuiteVersion|name|functionId|functionVersion|ruleExpr
    ----|----|----|----|----|----
    1 | 1 | aToTrue | 1 | 1 | `#!sql /** oops */ a -> a`
    1 | 1 | always1 | 2 | 1 | `#!sql a -> 1` 
    1 | **2** | aToTrue | 1 | **2** | `#!sql /** corrected */ a -> true()` 
    1 | **2** | always1 | 2 | **2** | **DELETED** 

To use these you replace the above with:

```{.scala #versioned}
import com.sparkutils.quality._
import simpleVersioning._

val rereadWithoutLambdas = readVersionedRulesFromDF(ruleDF,
  ...
)

val reReadLambdas = readVersionedLambdasFromDF(lambdaDF.toDF(),
  ...
)

val outputExpressions = readVersionedOutputExpressionsFromDF(outputDF,
  ...
)
val rereadWithLambdas = integrateVersionedLambdas(rereadWithoutLambdas, lambdas)
val (reread, missingOutputExpressions) = integrateVersionedOutputExpressions(rereadWithLambdas, outputExpressions)
```

The "readVersioned" functions modify the dataframe per the above logic to create full sets of ruleSuiteId + ruleSuiteVersion pairs.

The "integrateVersioned" functions will first try the same ruleSuiteId + ruleSuiteVersion pairs and were not present will take the next lowest available version.  This runs on the assumption you if didn't need to change any OutputExpressions for a new ruleSuite version why should you need to create fake entries.