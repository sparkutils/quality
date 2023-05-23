---
tags:
   - basic
   - getting started
   - beginner
boost: 5000
---

# Defining & Running your first RuleSuite

```scala
import com.sparkutils.quality._

// setup all the Quality sql functions
registerQualityFunctions()

// define a rule suite
val rules = RuleSuite(rsId, Seq(
  RuleSet(Id(50, 1), Seq(
    Rule(Id(100, 1), ExpressionRule("a % 2 == 0")),
    Rule(Id(100, 2), ExpressionRule("b + 20 < 10")),
    Rule(Id(100, 3), ExpressionRule("(100 * c) + d < e"))
  )),
  RuleSet(Id(50, 2), Seq(
    Rule(Id(100, 5), ExpressionRule("e > 60 or e < 30"))...
  )),
  RuleSet(Id(50, 3), Seq(
    Rule(Id(100, 9),ExpressionRule("i = 5")),
	...
    ))
  ), Seq(
    LambdaFunction("isReallyNull", "param -> isNull(param)", Id(200,134)),
    LambdaFunction("isGreaterThan", "(a, b) -> a > b", Id(201,131))
  ))

// add the ruleRunner expression to the DataFrame
val withEvaluatedRulesDF = sparkSession.read.parquet(...).
  withColumn("DataQuality", ruleRunner(rules))
  
withEvaluatedRulesDF.write. ... // or show, or count, or some other action  

```

Your expressions used, in dq/triggers, output expressions (for Rules and Folder) and lambda functions can contain any valid SQL that does not include Nondeterministic functions such as rand(), uuid() or indeed the Quality random and unique_id() functions.

??? info "3.4 & Sub queries"
    Prior to 3.4 exists, in, and scalar subqueries (correlated or not) could not be used in any Quality rule SQL snippets.
     
    3.4 has allowed the use of most sub query patterns, such as checking foreign keys via an exists in a dq rule where the data is to large for maps, or selecting the maximum matching value in an output expression.  There are some oddities like you must use an alias on the input dataframe if a correlated subquery also has the same field names, not doing so results in either silent failure or at best an 'Expression "XXX" is not an rvalue' compilation error.  The ruleEngineWithStruct transformer will automatically add an alias of 'main' to the input dataframe.  
    
    Lambdas however introduce some complications, 3.4 quite reasonably had no intention of supporting the kind of thing Quality is doing, so there is code making it work for the obvious use case of DRY using row attributes.  Attempting to use a lambda with parameters that are not attributes will result in an error e.g. given genMax:
    
    ```scala
    LambdaFunction("genMax", "ii -> select max(i_s.i) from tableName i_s where i_s.i > ii", Id(2404,1)))
    ```
    
    Calling with genMax(i) where i is an column attribute will work, calling with genMax(i * 1) will throw a QualityException. 
    
    This applies equally to Databricks 12.2 which back-ported this awesome functionality.

## withColumn is BAD - how else can I add columns?

I understand repeatedly calling withColumn/withColumnRenamed can cause performance issues due to excessive projections but how else can I add a RuleSuite in Spark?

```scala
// read a file and apply the rules storing results in the column DataQuality
sparkSession.read.parquet("theFilePath").
  transform(addDataQualityF(rules, "DataQuality"))

// read a file and apply the rules storing the overall result and details in the columns overallResult, dataQualityResults
sparkSession.read.parquet("theFilePath").
  transform(addOverallResultsAndDetailsF(rules, "overallResult", 
    "dataQualityResults"))
```

The transform functions allow easy chaining of operations on DataFrames.  However you can equally use the non "xxxxxF" functions such as addOverallResultsAndDetails with the same names to directly add columns and rule processing.

## Filtering the Results

The two most common cases for running DQ rules is to report on and filter out bad rows.  Filtering can be implemented for a RuleSuiteResult with:

```scala
withEvaluatedRulesDF.filter("DataQuality.overallResult = passed()")

```

Getting *all* of the rule results can be implemented with the flattenResults function:
```scala

val exploded = withEvaluatedRulesDF.select(expr("*"), 
  expr("explode(flattenResults(DataQuality))").
    as("struct")).select("*","struct.*")
```
Flatten results unpacks the resulting structure, including unpacking all the Id and Versions Ints combined into the single LongType for storage.

