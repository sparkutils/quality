---
tags: advanced
---

ExpressionRunner applies a RuleSuite over a dataset returning any expression.  When used with only aggregates it allows running dataset level checks, run after DQ it also allows statistics on individual rule results.   

It is important to note that if you are having multiple runners in the same data pipeline they should each use different RuleSuites.

RuleSuites are built per the normal DQ rules:

```{.scala #exampleCode}
    val dqRuleSuite = ...
    
    val aggregateRuleSuite = 
    
    val testDataDF = ...
      
    import frameless._
    import quality.implicits._
    
    // first add dataQuality, then ExpressionRunner
    val processed = com.sparkutils.quality.impl.ExpressionRunner.run(aggregateRuleSuite, 
        taddDataQuality(testDF, dqRuleSuite))
    
    val res = processed.selectExpr("expressionResults.*").as[GeneralExpressionsResult].head()
    assert(res == GeneralExpressionsResult(Id(10, 2), Map(Id(20, 1) -> Map(
      Id(30, 3) -> GeneralExpressionResult("499500", "BIGINT"),
      Id(31, 3) -> GeneralExpressionResult("500", "BIGINT")
    ))))
    
    val gres =
      processed.selectExpr("rule_result(expressionResults, pack_ints(10,2), pack_ints(20,1), pack_ints(31,3)) rr")
        .selectExpr("rr.*").as[GeneralExpressionResult].head
    
    assert(gres == GeneralExpressionResult("500", "BIGINT"))
```

??? warning "Don't mix aggregation functions with non-aggregation functions"
    Spark may complain before running an action, but it's also possible to produce incorrect results.

