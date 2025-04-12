---
tags: advanced
---

ExpressionRunner applies a RuleSuite over a dataset returning the results of any expression as yaml (json cannot support non string map keys), to return a single real type from all the rules use typedExpressionRunner instead.  When used with just aggregate expressions it allows running dataset level checks, run after DQ it also allows statistics on individual rule results.   

It is important to note that if you are having multiple runners in the same data pipeline they should each use different RuleSuites, and you should consider .cache'ing the intermediate results.

RuleSuites are built per the normal DQ rules and executed by adding an expressionRunner column:

```{.scala #exampleCode}
val dqRuleSuite = ...

val aggregateRuleSuite = 

val testDataDF = ...
  
import frameless._
import quality.implicits._

// first add dataQuality, then ExpressionRunner
val processed = taddDataQuality(sparkSession.range(1000).toDF, dqRuleSuite).select(expressionRunner(aggregateRuleSuite)).cache

val res = processed.selectExpr("expressionResults.*").as[GeneralExpressionsResult].head()
assert(res == GeneralExpressionsResult(Id(10, 2), Map(Id(20, 1) -> Map(
  Id(30, 3) -> GeneralExpressionResult("'499500'\n", "BIGINT"),
  Id(31, 3) -> GeneralExpressionResult("'500'\n", "BIGINT")
))))

val gres =
  processed.selectExpr("rule_result(expressionResults, pack_ints(10,2), pack_ints(20,1), pack_ints(31,3)) rr")
    .selectExpr("rr.*").as[GeneralExpressionResult].head

assert(gres == GeneralExpressionResult("'500'\n", "BIGINT"))
```

To retrieve results in the correct type use [from_yaml](../../sqlfunctions/#from_yaml) with the correct ddl.  As Spark needs an exact type for any expression you can't simply flatten or explode as with the other Quality runner types, each result can have it's own type.  As such it's recommended that the expressionRunner result row is cached and extraction is performed with one of the following pattern:

```scala
import sparkSession.implicits._
val t31_3 = processed.select(from_yaml(rule_result(col("expressionResults"), pack_ints(10,2), pack_ints(20,1), pack_ints(Id(31,3)), 'BIGINT'))).as[Long].head
```  

Or parse the GeneralExpressionResult map directly and:

```scala
val t31_3 = sparkSession.sql(s"from_yaml(${res.ruleSetResults(Id(20,1))(Id(30,3)).ruleResult}, 'BIGINT')").as[Long].head
```

or, finally, and perhaps as a last resort, to use snakeyaml and consume using the resulting java:

```scala
import org.yaml.snakeyaml.Yaml
val yaml = new Yaml();
val obj = yaml.load[Int](res.ruleSetResults(Id(20,1))(Id(30,3)).ruleResult).toLong;
println(obj);
```

However, as can be seen with the direct use of snakeyaml example the types may not always automatically align and, in this case or Decimal you risk losing precision.  Note that the yml spec allows the default implementation as the values have [no bit-length](https://yaml.org/spec/1.2.2/#10213-integer) for either integer or floats.

To increase precision / accuracy of the yaml types if you are not using from_yaml, you can provide the renderOptions map with useFullScalarType = 'true'.  This changes the output considerably:

```scala
val processed = taddDataQuality(sparkSession.range(1000).toDF, rowrs).select(expressionRunner(rs, renderOptions = Map("useFullScalarType" -> "true")))
 
val gres =
  processed.selectExpr("rule_result(expressionResults, pack_ints(10,2), pack_ints(20,1), pack_ints(31,3)) rr")
    .selectExpr("rr.*").as[GeneralExpressionResult].head

// NOTE: the extra type information allows snakeyaml to process Long directly without precision loss
assert(gres == GeneralExpressionResult("!!java.lang.Long '500'\n", "BIGINT"))

import org.yaml.snakeyaml.Yaml
val yaml = new Yaml();
val obj = yaml.load[Long](res.ruleSetResults(Id(20,1))(Id(30,3)).ruleResult);
println(obj);
```

NB: Decimal's will be stored with a java.math.BigDecimal type, rather than scala.math.BigDecimal

!!! info "You can add tags back in if needed"
    As using useFullScalarType -> true adds yaml type tags on all output scalars it can increase storage costs considerably, as such it's disabled by default.  
    
    It can however be retrieved by simply calling from_yaml and to_yaml again with it enabled if the end result should be used outside of Spark.

??? warning "Don't mix aggregation functions with non-aggregation functions"
    Spark may complain before running an action, but it's also possible to produce incorrect results.
    
    This is the equivalent of running:

    ```sql
    select *, sum(id) from table
    ```

    which will not work without group by's.

## strip_result_ddl

The resultType string is useful in debugging but may not be for storage, if you wish to trim this information from the results use the strip_result_ddl function.

This turns the result from GeneralExpressionResult into a simple string:

```{.scala #exampleCode}
    val stripped = processed.selectExpr("strip_result_ddl(expressionResults) rr")

    val strippedRes = stripped.selectExpr("rr.*").as[GeneralExpressionsResultNoDDL].head()
    assert(strippedRes == GeneralExpressionsResultNoDDL(Id(10, 2), Map(Id(20, 1) -> Map(
      Id(30, 3) -> "'499500'\n",
      Id(31, 3) -> "'500'\n")
    )))

    val strippedGres = {
      import sparkSession.implicits._
      stripped.selectExpr("rule_result(rr, pack_ints(10,2), pack_ints(20,1), pack_ints(31,3))")
        .as[String].head
    }

    assert(strippedGres == "'500'\n")
```