---
tags: advanced
---

# Aggregation Functions

Quality adds a number of aggregation primitives to allow building across dataset functionality similar to Deequ and others but philosophically staying true to the customisation approach used throughout the library.

At it's core all aggregations are formed using any number of aggExpr columns:

* aggExpr(filter, lambda sum, lambda result ) - allows filter expressions to be used to build up aggregated BIGINT (long) results with lambda functions, leveraging simple lambda functions (note count is currently only BIGINT / LongType):
```scala
// generates with an long id column from 1 to 20
val df = sparkSession.range(1, 20)
// filter odd numbers, add the them together with sumWith lambda for the 
// sum, then using resultsWith lambda variables divide them by the count 
// of filtered rows
val res = df.select(expr("aggExpr(id % 2 > 0, sumWith(sum -> sum + id), 
  resultsWith( (sum, count) -> sum / count ) )").as("aggExpr"))
res.show() // will show aggExpr with 10.0 as a result, 
  // sum + count would show 110..
```

The filter parameter lets you select rows you care about to aggregate, but does not stop you aggregating different filters in different columns and still process all columns in a single pass.  The sum function itself does the aggregation and finally the result function yields the last calculated result.  Both of these functions operate on MAPs of any key and value type.

Spark lambda functions are incompatible with aggregation wrt. type inference which requires that the type is specified to aggExpr as an optional default for any type other than bigint.

## Aggregation Lambda Functions 

* sumWith( lambda entry -> entry ) - processes for each matched row the lambda with the given ddl type which defaults to LongType 
* resultsWith( lambda (sum, count) -> ex ) - process results lambda with sum and count types passed in.
* inc( [expr] ) - increments the current sum either by default 1 or by expr using type LongType 
* meanF() - simple mean on the results, expecting sum and count type Long:
```scala
// generates with an long id column from 1 to 20
val df = sparkSession.range(1, 20)
// filter odd numbers, add the them together with inc lambda for the sum, then using meanF expression to divide them by the count of filtered rows
val res = df.select(expr("aggExpr(id % 2 > 0, inc(id), meanF() )").as("aggExpr"))
res.show() // will show aggExpr with 10.0 as a result, sum + count would show 110..
```
* mapWith( keyExpr, x ) - uses a map to group via keyExpr and apply x to each element:
```scala
// a counting example expr - group by and count distinct equivalent
expr("aggExpr('MAP<STRING, LONG>', 1 > 0, mapWith(date || ', ' || product, entry -> entry + 1 ), resultsWith( (sum, count) -> sum ) )").as("mapCountExpr")
// a summing example expr with embedded if's in the summing lambda for added fun
expr("aggExpr('MAP<STRING, DOUBLE>', 1 > 0, mapWith(date || ', ' || product, entry -> entry + IF(ccy='CHF', value, value * ccyrate)  ), returnSum() )").as("mapSumExpr")
```
* returnSum( ) - just returns the sum and ignores the count param, expands to resultsWith( (sum, count) -> sum)

## What about my pre 0.7.1 aggExpr functions using ddl type parameters?

Prior to 0.7.1 functions such as sumWith, mapWith, returnSum and returnWith each had their own ddl parameters.

0.7.1 has moved this to the optional first parameter of aggExpr itself, this both reduces duplication and is less error-prone (stopping different sum types in both sum and result).

The syntax _is_ backwards compatible however (with the exception of decimal handling), whilst the 'evaluate' (e.g. returnSum, returnWith) ddl type parameters are ignored the 'sum' type parameter is used for the whole of aggExpr's 'sum type'.

If you are using the deprecated sumWith('dll type', ..) and get differing types issues move the ddl to the first param of aggExpr.

## I get a strange error mentioning casts and type incompatiblity - what do?

In order to support the simplified single DDL parameter there are a number of Spark Expression tree re-writes taking place to 'inject' the right type.  These re-writes depend on a fixed format, this may change between Spark runtimes but they may also not work beyond the use cases they are tested against (see [AggregatesTest.scala](https://github.com/sparkutils/quality/blob/main/src/test/scala/com/sparkutils/qualityTests/AggregatesTest.scala) for the cases).

Spark creates different plans and Expression trees from the simplified vs. the pre 0.7.1 versions, this could lead to unexpected re-write issues.

If an sql was working pre 0.7.1 with the deprecated syntax but fails with the simplified or indeed you simply wish to test out if the previous syntax would have worked you can supply 'NO_REWRITE' for the first parameter of aggExpr (instead of DDL) in addition to supplying the other two DDL's directly.  The ability to provide types is present for inc, meanF, returnSum and returnWith as before.

!!! note
    inc('DDL', expression) does not work with NO_REWRITE, as such it throws an exception telling you to use the default approach.  You can use an attribute directly with NO_REWRITE just not expressions

## Type Lookup and Monoidal Merging

This section is very advanced but may be needed in a deeply nested type is to be aggregated.

### Type Lookup
aggExpr, mapWith, sumWith and returnSum all rely on type lookup.  The implementation uses sparks in-built DDL parsing to get types, but can be extended by supplying a custom function when registering functions e.g.:
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
