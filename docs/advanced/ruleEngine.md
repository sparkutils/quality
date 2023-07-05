---
tags: advanced
---
   
Quality provides a basic rule engine for data quality rules the output of each rule however is always translated to RuleResult, encoded and persisted for audit reasons.

The ruleEngineRunner function however allows you to take an action based on the passing of a rule and, via salience, choose the most appropriate output for a given row.

You can understand QualityRules as a large scale auditable SQL case statement with "when" being the trigger rule and the "then" as the output expression.

RuleSuites are built per the normal DQ rules however a RuleResultProcessor is supplied:

```scala
  val ruleResultProcessor = 
    RunOnPassProcessor(salience, Id(outputId, outputVersion), RuleLogicUtils.expr("array(account_row('from', account), account_row('to', 'other_account1'))")))
  val rule = Rule(Id(id, version), expressionRule, ruleResultProcessor)
  val ruleSuite = RuleSuite(Id(ruleSuiteId, ruleSuiteVersion), Seq(
      RuleSet(Id(ruleSetId, ruleSetVersion), Seq(rule)
      )))

  val rer = ruleEngineRunner(ruleSuite,
      DataType.fromDDL("ARRAY<STRUCT<`transfer_type`: STRING, `account`: STRING>>"))
  
  val testDataDF = ...
  
  val outdf = testDataDF.withColumn("together", rer).selectExpr("*", "together.result")
```

The ruleEngineRunner takes a DataType parameter that must describe the type of the result column type.  An additional salientRule column is available that packs three the Id's that represent the ruleId chosen by salience.  If this is null then _no_ rule was triggered and the output column will also be null (verifiable via debug mode), if however there is an entry but the output is null then this signifies that the output expression produced a null.

The salientRule column may be pulled apart down to the id number and versions via the unpack expression or unpackIdTriple to unpack the lot in one go.  If you are using frameless encoders these longs can be converted to a triple of Id's.  

The salience parameter to the RunOnPassProcessor is used to ensure the lowest value is returned for a ruleSuite.  It is the responsibility of the rule configuration to ensure there can only be one output.

All of the existing functionality, lambadas etc. can be used to customise the results and, as per the normal DQ processing, is run in-process across the clusters when the spark action is taken (like writing the dataframe to disk).

## Serializing

The serializing approach uses the same functions as normal DQ RuleSuites, the only difference is you should use toDS and provide the two additional ruleEngine parameters when reading from a DF:

```scala
  val withoutLambdasAndOutputExpressions = readRulesFromDF(rulesDF,
      col("ruleSuiteId"),
      col("ruleSuiteVersion"),
      col("ruleSetId"),
      col("ruleSetVersion"),
      col("ruleId"),
      col("ruleVersion"),
      col("ruleExpr"),
      col("ruleEngineSalience"), 
      col("ruleEngineId"),
      col("ruleEngineVersion")
    )

  val lambdas = ...
  
  val outputExpressions = readOutputExpressionsFromDF(so.toDF(),
    col("ruleExpr"),
    col("functionId"),
    col("functionVersion"),
    col("ruleSuiteId"),
    col("ruleSuiteVersion")
  )

  val (ruleMap, missing) = integrateOutputExpressions(withoutLambdasAndOutputExpressions, outputExpressions)    

```

The ruleExpr is only run for the lowest ruleEngineSalience result of any passing ruleExpr.  The missing result will contain any output expressions specified by a rule which do not exist in the output expression dataframe based by rulesuite id, if your rulesuite id is not present in the missing entries your RuleSuite is good to go.

The rest of the serialization functions to combine lambdas etc. work as per normal DQ rules allowing you to use lambda functions in your QualityRules output rules as well.

The result of toDS will contain the three ruleEngine fields, you can simply drop them if they are not needed.

## Debugging

The RuleResult's indicate if a rule has not triggered but in the case of multiple matching rules it can be useful to see which rules would have been chosen.

To enable this you can add the debugMode parameter to the ruleEngineRunner:

```scala
  val rer = ruleEngineRunner(ruleSuite,
      DataType.fromDDL("ARRAY<STRUCT<`transfer_type`: STRING, `account`: STRING>>"),
      debugMode = true)
```

This changes the output column 'result' field type to:

```sql
ARRAY<STRUCT<`salience`: INTEGER, `result`: ARRAY<ORIGINGALRESULTTYPE>>
```

!!! note "Why do I have a null"
    There are two cases where you may get a null result:
    
    1. no rules have matched (you can verify this as you'll have no passed() rules).
    2. your rule actually returned a null (you can verify this by putting on debug mode, you'll see a salience but no result)

## flatten_rule_results
    
```scala
  val outdf = testDataDF.withColumn("together", rer).selectExpr("explode(flatten_rule_results(together)) as expl").selectExpr("expl.*")
```

This sql function behaves the same way as per flatten_results, however there are now two structures to 'explode'.  debugRules works as expected here as well.

## resolveWith

!!! warning "Use with care - very experimental"
    The resolveWith functionality has several issues with Spark compatibility which may lead to code failing when it looks like it should work.
    Known issues:
    
    1. Using filter then count will stop necessary attributes being produced for resolving, Spark optimises them out as count doesn't need them, however the rules definitely do need some attributes to be useful.
    2. You may not select different attributes, remove any, re-order them, or add extra attributes, this is likely to cause failure in show'ing or write'ing
    3. Spark is free to optimise other actions than just count, ymmv in which ones work.     
    4. The 0.1.0 implementation of update_field (based on the Spark impl) does not work in some circumstances (testSimpleProductionRules will fail) - see #36

resolveWith attempts to improve performance of planning for general spark operations by first using a reduced plan against the source dataframe.  The resulting Expression will have all functions and attributes resolved and is hidden from further processing by Spark until your rules actually run. 

```scala
  val testDataDF = ....

  val rer = ruleEngineRunner(ruleSuite,
      DataType.fromDDL(DDL), debugMode = debugMode, resolveWith = resolveWith = Some(testDataDF))

  val withRules = rer.withColumn("ruleResults", rer)

  // ... use the rules
```

### Why is this needed?

For RuleSuites with 1000s of triggers the effort for Spark to prepare the rules is significant.  In tests 1k rule with 50 field evalutaions is already sufficient to cause a delay of over 1m for each action (show, write, count etc.) and the size of the data being processed is not relevant.

After building the action QualityRules scale and perform as expected, but that initial costs of 1m per action is significant as it can only be improved by higher spec drivers.

resolveWith, if it works for given use case, drastically reduces this cost, the above 1k example is a 30s evaluation up front and far less cost for each further action.

With the rather horrible 1k rule example the clock time of running 1k rows through 1k rules with a simple show, then count and write for actions was 6m15s on an Azure b4ms, using resolveWith brings this down to 1m30s for the same actions.  Still not blazingly fast of course, but far more tolerable and becomes suitable for smaller batch jobs.

### Any reason why I shouldn't try it?

Not really but for production use cases where your trigger and output rules complexity is low you should prefer to not use it, it's likely fast enough and this solution is very much experimental.

You definitely shouldn't use it when using relation or table fields in your expressions e.g. table.field this does not work (verify this by running JoinValidationTest using evalCodeGens instead of evalCodeGensNoResolve).  There be dragons.  This is known to fail on *all* OSS builds and OSS runtimes (up to and including 3.2.0).  10.2.dbr and 9.1.dbr *actually do work* running the tests in notebooks with resolveWith and relations (the test itself is not built for this however to ensure cross compilation on the OSS base).

## forceRunnerEval

By default, QualityRules runs with an optimised wholestage codegen wherever possible.  This works by breaking out the nested structure of a RuleSuite into multiple index, salience and id arrays which are fixed for the duration of an action.  Whilst this reduces the overhead of array and temporary structure creation the compilation also unrolls the evaluation of trigger rules allowing jit optimisations to kick in.

Using large RuleSuites, however, may cause large compilation times which are unsuitable for smaller batches, as such you can force the interpreted path to be used by setting this parameter to true.  Individual trigger and output expressions are still compiled but the evaluation will not be.

