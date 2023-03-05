---
tags:
   - model
---

# Meta Rulesets?

Quality introduces a "Meta Ruleset" approach for added automation.  Meta Rule sets evaluate each column of a DataFrame to see if a Rule should be generated for that column.

Null checks, type checks etc. may all be applied generically without laboriously copying the rule for each applicable column, just define a single argument lambda expression.  In order for this to work and be extensible you require stable ordering for each column used.

```scala
// if you wish to use Meta Rule Sets 
val metaRuleSets = readMetaRuleSetsFromDF(metaRuleDF,
// an sql filter of the schema from a provided dataframe - name, 
//datatype (as DDL) and nullable can be filtered
    col("columnFilter"), 
// single arg lambda to apply to all fields from the column filter
    col("ruleExpr"), 
    col("ruleSetId"),
    col("ruleSetVersion"),
    col("ruleSuiteId"),
    col("ruleSuiteVersion")
  )

// make sure we use the correct rule suites for the dataset, e.g.
val filteredRuleSuites: RuleSuiteMap = Map(ruleSuiteId -> rules)

val theDataframe = sparkSession.read.parquet("theFilePath")

// Guarantee each column always returns the same unique position
val stablePositionsFromColumnNames: String => Int = ???  

// filter theDataframe columns and generate rules for each Meta 
//  RuleSet and re-integrate them 
val newRuleSuiteMap = integrateMetaRuleSets(theDataframe, filteredRuleSuites, 
  metaRuleSets, stablePositionsFromColumnNames)

```

An optional last paramater for integrateMetaRuleSets allows transformation of a generated column dataframe, allowing joins with other lookup tables for the column definition or applicable rules to generate for the column for example.
