---
tags: advanced
---

# Map Functions

A typical use case for processing DQ rules is that of cached value processing, reference data lookups or industry code checks etc.

Quality's map functions reproduce the result of joining datasets but, once they are loaded, guarantees in memory operation only - no merges or joins required.  However, for larger data lookups correlated subqueries or [Bloom Filters](blooms.md) should be preferred, or simply use joins.

Similarly, for cases involving more logic than a simple equality check you must use joins or starting in 3.4 (DBR 12.2) scalar sub queries, see [View Loader](viewLoader.md) for a way to manage the loading of views. 

!!! info ":new:{.pulseABit} Map functions on 0.2.0 Spark 4"
    Map functions are now able to use stableName variable names and register multiple MapLookups (a simple String for use in SQLs), each function is changed accordingly adding stableName parameters registered as Spark 4 Variables:
    
    ```scala
    def loadMaps(configs: Seq[MapConfig]): String 
    // gains the ability to specify the name to use, which returns stableName
    def loadMaps(configs: Seq[MapConfig], stableName: String): String
    // and, similarly, 
    def mapLookupsFromDFs(creators: Map[String, MapCreator]): String
    // gains this overloaded version
    def mapLookupsFromDFs(creators: Map[String, MapCreator], stableName: String): String
    ```
    
    finally, the SQL lookup function itself requires the returned stableName:

    ```sql
    map_lookup('mapid', expr, mapLookupsVar)
    map_contains('mapid', expr, mapLookupsVar)
    ```

    with registerMapLookupsAndFunction now becoming a no-op and is retained for compatibilty only.

## Map Loading

The interface and config row data types is similar to that of [View Loader](viewLoader.md) with [loadMapConfigs](../../site/scaladocs/com/sparkutils/quality/impl/mapLookup/MapLookupImports.html#loadMapConfigs(loader:com.sparkutils.quality.DataFrameLoader,viewDF:org.apache.spark.sql.DataFrame,ruleSuiteIdColumn:org.apache.spark.sql.Column,ruleSuiteVersionColumn:org.apache.spark.sql.Column,ruleSuiteId:com.sparkutils.quality.Id,name:org.apache.spark.sql.Column,token:org.apache.spark.sql.Column,filter:org.apache.spark.sql.Column,sql:org.apache.spark.sql.Column,key:org.apache.spark.sql.Column,value:org.apache.spark.sql.Column):(Seq[com.sparkutils.quality.impl.mapLookup.MapConfig],Set[String]) ) accepting these additional columns:

```scala
val (mapConfigs, couldNotLoad) = loadMapConfigs(loader, config.toDF(), expr("id.id"), expr("id.version"), Id(1,1),
  col("name"),col("token"),col("filter"),col("sql"),col("key"),col("value")
)

val maps = loadMaps(mapConfigs)
```

with couldNotLoad holding a set of configuration rows that aren't possible to load (neither a DataFrameLoader token nor an sql).

[loadMaps](../../site/scaladocs/com/sparkutils/quality/impl/mapLookup/MapLookupImports.html#loadMaps(configs:Seq[com.sparkutils.quality.impl.mapLookup.MapConfig]):MapLookupImports.this.MapLookups) will process the resulting dataframe using key and value as sql expressions in exactly the same way as mapLookupFromDFs, as such they must be valid expressions against the source dataframe.  Views first loaded via view loader are available when executing the sql column (when token is null).

## Building the Lookup Maps Directly

In order to lookup values in the maps Quality requires a map of map id's to the actual maps.

```scala
// create a map from ID to a MapCreator type with the dataframe and underlying 
// columns, including returning structures / maps etc.
val lookups = mapLookupsFromDFs(Map(
      "countryCode" -> ( () => {
        val df = countryCodeCCY.toDF("country", "funnycheck", "ccy")
        (df, column("country"), functions)("struct(funnycheck, ccy)"))
      } ),
      "ccyRate" -> ( () => {
        val df = ccyRate.toDF("ccy", "rate")
        (df, column("ccy"), column("rate"))
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
* map_lookup('map name', x) - looks up x against the map specified in map name, full type transparency from the underlying map values are supported including deeply nested structures
```scala
// show the map of data 'country' field against country code and get back the currency
df.select(col("*"), expr("map_lookup('countryCode', country).ccy")).show()
```
* map_contains('map name', x) - returns true or false if an item is present as a key in the map
