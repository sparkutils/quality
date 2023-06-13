---
tags: advanced
---

# Map Functions

A typical use case for processing DQ rules is that of cached value processing, reference data lookups or industry code checks etc.

Quality's map functions reproduce the result of joining datasets but guarantees in memory operation only once they are loaded, no merges or joins required.  However for larger data lookups either [Bloom Filters](blooms.md) should be preferred or simply use joins.

Similarly, for cases involving more logic than a simple equality check you must use joins or starting in 3.4 (DBR 12.2) scalar sub queries. 

## Building the Lookup Maps

In order to lookup values in the maps Quality requires a map of map id's to the actual maps.

```scala
// create a map from ID to a MapCreator type with the dataframe and underlying 
// columns, including returning structures / maps etc.
val lookups = mapLookupsFromDFs(Map(
      "countryCode" -> ( () => {
        val df = countryCodeCCY.toDF("country", "funnycheck", "ccy")
        (df, new Column("country"), functions.expr("struct(funnycheck, ccy)"))
      } ),
      "ccyRate" -> ( () => {
        val df = ccyRate.toDF("ccy", "rate")
        (df, new Column("ccy"), new Column("rate"))
      })
    ))
registerMapLookupsAndFunction(lookups)
```

In the countryCode map lookup case we are creating a map from country to a structure (funnycheck, ccy), whereas the ccyRate is a simple lookup between ccy and it's rate at point of loading.

Map creation is not lazy and is forced at time of calling the registerMap... function, for streaming jobs this may be unacceptable.  Prefer to use new map id's and merge old sets if you need to guarantee repeated calls to registerMapLookupsAndFunctions are working with up to date data.

It's possible to have multiple fields used as the key, where all must match, just use struct in the same way as the value example above. 

!!! note
    Repeated calls and streaming use cases have not been thoroughly tested, the Spark distribution method guarantees an object can be broadcast but no merging is automatically possible, users would be required to code this by hand.

## Expressions which take expression parameters
* mapLookup('map name', x) - looks up x against the map specified in map name, full type transparency from the underlying map values are supported including deeply nested structures
```scala
// show the map of data 'country' field against country code and get back the currency
df.select(col("*"), expr("mapLookup('countryCode', country).ccy")).show()
```
* mapContains('map name', x) - returns true or false if an item is present as a key in the map
