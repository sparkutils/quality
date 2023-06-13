As of Spark 3.4 sub queries become a great way to provide lookups and transformation logic in rules.  In order to support an easier use of views the following functions have been added in 0.1.0:

```scala
val (viewConfigs, failed) = loadViewConfigs(loader, config.toDF(), expr("id.id"), expr("id.version"), Id(1,1),
      col("name"),col("token"),col("filter"),col("sql")
    )

val results = loadViews(viewConfigs)
``` 

[loadViewConfigs]( ../../site/scaladocs/com/sparkutils/quality/impl/ViewLoader/index.html ) takes a DataFrameLoader as a parameter allowing Quality to load tables based on your integration logic.  There are two flavours, one expecting a table with the following schema:

```sql
STRUCT< name : STRING, token : STRING nullable, filter : STRING nullable, sql: STRING nullable> 
```

and the other tied to a RuleSuite:

```sql
STRUCT< ruleSuiteId: INT, ruleSuiteVersion: INT, name : STRING, token : STRING nullable, filter : STRING nullable, sql: STRING nullable> 
```

both versions return any rows for which token and sql are both null in the failed result and the resulting configuration in viewConfigs.

Where token is present the loader will be called for it and the filter column applied (allowing re-use).

After loading the ViewConfig's the loadViews function can be called, registering all the views via createOrReplaceTempView and returning a set of replaced views, failedToLoadDueToCycles and notLoadedViews, a set of unloaded views.  In the event that views refer to other views not present in ViewConfig a MissingViewAnalysisException will be thrown, ViewLoaderAnalysisException for other analysis exceptions, as will parsing exceptions as per normal Spark.

[loadViews](../../site/scaladocs/com/sparkutils/quality/impl/ViewLoader/index.html ) will attempt to automatically attempt to resolve ViewConfigs that depend on other ViewConfigs, where there is a cycle that is 2x the number of ViewConfigs the call will return with failedToLoadDueToCycles as true.

These calls must be made before running any dq, engine or folder using views.   

   

