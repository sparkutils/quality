---
tags: advanced
---
   
The ruleFolderRunner function uses the same data formats and structures as the ruleEngineRunner (with the exception of RuleFolderResult) however it allows you to "fold" results over many matching rules.

In contrast to ruleEngineRunner, which uses salience to select which output expression to run, ruleFolderRunner uses salience to order the execution of each matching output expression.  To facilitate this OutputExpressions in the ruleFolderRunner must be lambdas with one parameter.

ruleFolderRunner takes a starter Column, which is evaluated against the row and then is passed as the parameter to the OutputExpression lambdas, in turn the result of these output lambdas is then fed in to the next matching OutputExpression and folded over until the last is run, which is returned.

When using debugMode you get the salience and each output returned in the resulting array, as with ruleEngineRunner the Encoder derivations for RuleFolderResult work with both T and Seq[(Int, T)] where the Int is salience.

RuleSuites are built per the normal DQ rules however a RuleResultProcessor is supplied with Lambda OutputExpressions:

```{.scala #exampleCode}
  val ruleResultProcessor = 
    RunOnPassProcessor(salience, Id(outputId, outputVersion), 
      RuleLogicUtils.expr("thecurrent -> updateField(thecurrent, 'account', concat(thecurrent.account, '_suffix') )")))
  val rule = Rule(Id(id, version), expressionRule, ruleResultProcessor)
  val ruleSuite = RuleSuite(Id(ruleSuiteId, ruleSuiteVersion), Seq(
      RuleSet(Id(ruleSetId, ruleSetVersion), Seq(rule)
      )))

  val rer = ruleFolderRunner(ruleSuite,
      struct($"transfer_type", $"account"))
  
  val testDataDF = ...
  
  val outdf = testDataDF.withColumn("together", rer).selectExpr("*", "together.result")
```

You may use multiple path, expression combinations, to change multiple fields at once - this will be faster than nesting results.

The use of lambda expressions allows you full control of your output expression - but it can be a bit verbose.  The common use case of defaulting is supported however e.g. the following are equivalent:

```sql
thecurrent -> updateField(thecurrent, 'account', concat(thecurrent.account, '_suffix') )
set( account, concat(currentResult.account, '_suffix') )
```

The set syntax defaults the name of the lambda variable to "currentResult" and removes the odd looking quotes around the variables. 


??? warning "Don't use 'current' for a variable on 2.4"
    It may be tempting to use 'current' as your lambda variable name, but this causes problems on 2.4 - every other version doesn't care.

??? warning "Don't use resolveWith on 2.4"
    2.4 will NPE using withResolve, this does not occur on more recent Spark versions

??? warning "Don't use select(*, ruleFolderRunner)"
    Spark will not NPE using withColumn but will using select(expr("*"), ruleFolderRunner(ruleSuite)).  In order to thread the types through the resolving needs an additional projection, if you must avoid withColumn (e.g for performance reasons) then you may specify the DDL via the useType parameter.

## flattenFolderResults

```scala
  val outdf = testDataDF.withColumn("together", rer).selectExpr("explode(flattenFolderResults(together)) as expl").selectExpr("expl.result")
```

This sql function behaves the same way as per flattenRuleResults with debugRules working as expected.

## resolveWith

!!! warning "Use with care - very experimental"
    The resolveWith functionality has several issues with Spark compatibility which may lead to code failing when it looks like it should work.
    Known issues:
    
    1. Using filter then count will stop necessary attributes being produced for resolving, Spark optimises them out as count doesn't need them, however the rules definitely do need some attributes to be useful.
    2. You may not select different attributes, remove any, re-order them, or add extra attributes, this is likely to cause failure in show'ing or write'ing
    3. Spark is free to optimise other actions than just count, ymmv in which ones work.     
     
resolveWith attempts to improve performance of planning for general spark operations by first using a reduced plan against the source dataframe.  The resulting Expression will have all functions and attributes resolved and is hidden from further processing by Spark until your rules actually run. 

```scala
  val testDataDF = ....

  val rer = ruleEngineRunner(sparkSession.sparkContext.broadcast(ruleSuite),
      DataType.fromDDL(DDL), debugMode = debugMode, resolveWith = resolveWith = Some(testDataDF))

  val withRules = rer.withColumn("ruleResults", rer)

  // ... use the rules
```

You definitely shouldn't use it when using relation or table fields in your expressions e.g. table.field this does not work (verify this by running JoinValidationTest using evalCodeGens instead of evalCodeGensNoResolve).  There be dragons.  This is known to fail on *all* OSS builds and OSS runtimes (up to and including 3.2.0).  10.2.dbr and 9.1.dbr *actually do work* running the tests in notebooks with resolveWith and relations (the test itself is not built for this however to ensure cross compilation on the OSS base).
