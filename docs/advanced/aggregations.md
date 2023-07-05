---
tags: advanced
---

## Aggregation Functions

Quality adds a number of aggregation primitives to allow building across dataset functionality similar to Deequ and others but philosophically staying true to the customisation approach used throughout the library.

You can aggregate using any number of agg_expr columns:

* agg_expr(ddl type, filter, lambda sum, lambda result ) - allows filter expressions to be used to build up aggregated BIGINT (long) results with lambda functions, leveraging simple lambda functions (note count is currently only BIGINT / LongType):
```scala
// generates with an long id column from 1 to 20
val df = sparkSession.range(1, 20)
// filter odd numbers, add the them together with sumWith lambda for the 
// sum, then using resultsWith lambda variables divide them by the count 
// of filtered rows
val res = df.select(expr("agg_expr('BIGINT', id % 2 > 0, sum_with(sum -> sum + id), "+
  "results_with( (sum, count) -> sum / count ) )").as("aggExpr"))
res.show() // will show aggExpr with 10.0 as a result, 
  // sum + count would show 110..
```

The filter parameter lets you select rows you care about to aggregate, but does not stop you aggregating different filters in different columns and still process all columns in a single pass.  The sum function itself does the aggregation and finally the result function yields the last calculated result.  Both of these functions operate on MAPs of any key and value type.

Spark lambda functions are incompatible with aggregation wrt. type inference which requires that the type is specified to agg_expr, it defaults to bigint when not specified.

The [ExpressionRunner](expressionRunner.md) provides a convenient way to manage multiple agg_expr aggregations in a single pass action via a RuleSuite, just like DQ rules. 

## Aggregation Lambda Functions 

* sum_with( lambda entry -> entry ) - processes for each matched row the lambda with the given ddl type which defaults to LongType 
* results_with( lambda (sum, count) -> ex ) - process results lambda with sum and count types passed in.
* inc( [expr] ) - increments the current sum either by default 1 or by expr using type LongType 
* meanF() - simple mean on the results, expecting sum and count type Long:
```scala
// generates with an long id column from 1 to 20
val df = sparkSession.range(1, 20)
// filter odd numbers, add the them together with inc lambda for the sum, then using meanF expression to divide them by the count of filtered rows
val res = df.select(expr("agg_expr(id % 2 > 0, inc(id), meanF() )").as("aggExpr"))
res.show() // will show aggExpr with 10.0 as a result, sum + count would show 110..
```
* map_with( keyExpr, x ) - uses a map to group via keyExpr and apply x to each element:
```scala
// a counting example expr - group by and count distinct equivalent
expr("agg_expr('MAP<STRING, LONG>', 1 > 0, map_with(date || ', ' || product, entry -> entry + 1 ), results_with( (sum, count) -> sum ) )").as("mapCountExpr")
// a summing example expr with embedded if's in the summing lambda for added fun
expr("agg_expr('MAP<STRING, DOUBLE>', 1 > 0, map_with(date || ', ' || product, entry -> entry + IF(ccy='CHF', value, value * ccyrate)  ), return_sum() )").as("mapSumExpr")
```
* return_sum( ) - just returns the sum and ignores the count param, expands to results_with( (sum, count) -> sum)

## Column DSL

The same functionality is available in the functions package e.g.:

```scala
import com.sparkutils.quality.functions._
val df: DataFrame = ...
df.select(agg_expr(DecimalType(38,18), df("dec").isNotNull, sum_with(entry => df("dec") + entry ), return_sum) as "agg")
``` 

## Type Lookup and Monoidal Merging

This section is very advanced but may be needed in a deeply nested type is to be aggregated.

### Type Lookup

agg_expr, map_with, sum_with and return_sum all rely on type lookup.  The implementation uses sparks in-built DDL parsing to get types, but can be extended by supplying a custom function when registering functions e.g.:
```scala
registerQualityFunctions(parseTypes = (str: String) => defaultParseTypes(str).orElse( logic goes here ) /* Option[DataType] */)
```

### Monoidal Merging

Unlike type lookup custom merging could well be required for special types.  Aggregation (as well as MapMerging and MapTransform) require a Zero value the defaultZero function can be extended or overwritten and passed into registerFunctions as per parseTypes.
The defaultAdd function uses itself with an extension function parameter in order to supply map value monoidal associative add.  

!!! note
    This works great for Maps and default numeric types but it requires custom monoidal 'add' functions to be provided for merging complex types.
   
    Whilst zero returns a value to use as zero you may need to recurse for nested structures of zero, add requires defining Expressions and takes a left and right Expression to perform it:
	
    ```scala
    DataType => Option[( Expression, Expression ) => Expression]
    ```
	
!!! warning
    This is an area of functionality you should avoid unless needed as it often requires deep knowledge of Spark internals.  There be dragons.
