---
tags:
   - basic
   - getting started
   - beginner
---

## Migrating from 0.1.x to 0.2.0

Quality, as of 0.2.0, is delivered via 4 jars:

* quality - effectively the same as Quality pre 0.2.0
* quality_api - the basic interface for quality, included by the normal quality library  
* quality_testshade - the test shaded uber package for testing and exploration (this shade both the quality and quality_api jars)
* quality_connect_testshade - the connect test shaded uber package for testing and exploration with Spark 4 / DBR 17.3 server extensions.  (this only shades the quality_api jar) 

Existing users should continue to depend on the quality jar.  Connect users on Spark 4 / DBR 17.3 and above however can also make remote calls by just depending on quality_api.

The following functional areas are only present in the full quality jar:

- Bloom filters, as are 
- Processors, 
- documentation functions, 
- validation functions
- and a number of classic only utility functions.  
 
In order to use the functions and related data types that are not supported in the connect compatible api use classicFunctions:

```scala
import com.sparkutils.quality.classicFunctions._ 
```

The classicFunctions rule, engine, folder and expression runner functions will use connect where required and classic where possible.  Functions which are only possible to use with classic are annotated with ClassicOnly.

??? note "registerQualityFunctions has no params?"
    com.sparkutils.quality.registerQualityFunctions no longer takes parameters.  On classic, non quality_api, it forwards to the com.sparkutils.quality.classicFunction.registerQualityFunctions's default implementation.
    When using quality_api via connect it's a no-op, as the functions exist on the server.

### Breaking Change Spark 4

The SQL function's map_lookup and map_contains take the additional parameter of the MapLookup Spark SQL Variable name:

```sql
map_lookup('mapid', expr, mapLookupsVar)
map_contains('mapid', expr, mapLookupsVar)
```

### Spark 4, Connect and Remote Calls

The quality_api jars can be used as a remote interface to Quality functionality running as a [SparkSessionExtension](#using-the-sql-functions-on-spark-thrift-hive-servers).

The new quality_api jar provides a very thin and stable interface that simply forwards execution to the SparkSessionExtension on the driver and acts as an example of what other language support should provide.

In this pattern the 'client' application only needs to depend on the quality_api jar, allowing the exact Quality implementation 'server' to be upgraded.  Under OSS Spark 4:

- spark.sql.artifact.isolation.enabled and 
- spark.sql.artifact.isolation.alwaysApplyClassloader 
 
allow multiple client applications to exist and safely share the server, enabled by default when using the Spark Connect server or  "spark.api.mode=connect". Databricks Shared Compute clusters with [Lakeguard](https://docs.databricks.com/aws/en/compute/lakeguard) provide the same isolation.

## Building The Library

* fork, 
* use the Scala dev environment of your choice,
* or build directly using Maven

If using IntelliJ the ide / nailgun compiler combo will not be able to handle the project correctly.
Use the Maven clean and compile on an appropriate profile, following the compile IDEA will work correctly. 

### Building via commandline

For OSS versions (non Databricks runtime - dbr):

```bash
mvn --batch-mode --errors --fail-at-end --show-version -DinstallAtEnd=true -DdeployAtEnd=true -DskipTests install -P Spark321
```

but dbr versions will not be able to run tests from the command line (typically not an issue in intellij):

```bash
mvn --batch-mode --errors --fail-at-end --show-version -DinstallAtEnd=true -DdeployAtEnd=true -DskipTests clean install -P 10.4.dbr
```

You may also build the shaded uber test jar for easy testing in Spark clusters for each profile:

```bash
mvn -f testShades/pom.xml --batch-mode --errors --fail-at-end --show-version -DinstallAtEnd=true -DdeployAtEnd=true -Dmaven.test.skip=true clean install -P 10.4.dbr
```

The uber test jar artefact starts with 'quality_testshade_' instead of just 'quality_' and is located in the testShades/target/ directory of a given build.  This is also true for the artefacts of a runtime build job within a full build gitlab pipeline.  All of the required jar's are shaded so you can quickly jump into using Quality in [notebooks for example](running_on_databricks/#testing-out-quality-via-notebooks).

### Building in IntelliJ

After selecting the profile of your choice (only use OSS profiles for test runs and debugging) use the Maven view to run 
a clean and compile.  server_shared and api_stub are not correctly built, IntelliJ will correctly show compilation in the 
editor's and dependency views but actually compiling will sometimes fail, where running Maven compile will succeed.

You may also have to invalidate caches and restart if moving between 0.1 and 0.2 versions of Quality source.

## Running the tests

In order to run the tests you must follow [these instructions](https://github.com/globalmentor/hadoop-bare-naked-local-fs/issues/2#issuecomment-1444453024) to create a fake winutils.

Also ensure only the correct target Maven profile and source directories are enabled in your IDE of choice. 

The performance tests are not automated and must be manually run when needed.

When running tests on jdk 17/21 you also need to add the following startup parameters:

```
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.net=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED
--add-opens=java.base/sun.security.action=ALL-UNNAMED
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED
```

Also for Spark 4 builds requiring 17/21 you must use Scala SDK 2.13.12 or similar which supports higher jdk versions. 

## Build tool dependencies

Quality is cross compiled for different versions of Spark, Scala _and_ runtimes such as Databricks.  The format for artifact's is:

```
quality_RUNTIME_SPARKCOMPATVERSION_SCALACOMPATVERSION-VERSION.jar
```

e.g.

```
quality_4.1.0.oss_4.1_2.13-0.2.0.jar
```

The build poms generate those variables via maven profiles, but you are advised to use properties to configure e.g. for Maven:

```xml
<dependency>
    <groupId>com.sparkutils</groupId>
    <artifactId>quality_${qualityRuntime}${sparkShortVersion}_${scalaCompatVersion}</artifactId>
    <version>${qualityVersion}</version>
</dependency>
```

The full list of supported runtimes is below:

| Spark Version | sparkShortVersion | qualityRuntime                 | scalaCompatVersion |
|---------------|-------------------|--------------------------------|--------------------| 
| 3.1.3         | 3.1               |                                | 2.12               | 
| 3.2.0         | 3.2               |                                | 2.12               | 
| 3.2.1         | 3.2               | 3.2.1.oss_                     | 2.12               | 
| 3.3.2         | 3.3               | 3.3.2.oss_                     | 2.12               | 
| 3.3.2         | 3.3               | 12.2.dbr_                      | 2.12               |
| 3.4.1         | 3.4               | 3.4.1.oss_                     | 2.12               |
| 3.4.1         | 3.4               | 13.3.dbr_                      | 2.12               |
| 3.5.0         | 3.5               | 3.5.0.oss_                     | 2.12               |
| 3.5.0         | 3.5               | 14.3.dbr_                      | 2.12               |
| 3.5.0         | 3.5               | 15.4.dbr_                      | 2.12               |
| 3.5.0         | 3.5               | 16.4.dbr_                      | 2.12               |
| 4.0.0         | 4.0               | 4.0.0.oss_                     | 2.13               |
| 4.0.0         | 4.0               | 17.3.dbr_                      | 2.13               |
| 4.0.0         | 4.0               | api_4.0.0.oss_                 | 2.13               |
| 4.0.0         | 4.0               | api_17.3.dbr_                  | 2.13               |
| 4.1.0         | 4.1               | api_4.1.0.oss_                 | 2.13               |
| 4.1.0         | 4.1               | 4.1.0.oss_                     | 2.13               |
| 4.1.0         | 4.1               | 18.3.dbr_                      | 2.13               |
| 4.2.0         | 4.2               | api_4.2.0.oss_                 | 2.13               |
| 4.2.0         | 4.2               | 4.2.0.oss_                     | 2.13               |
| 4.2.0         | 4.2               | 18.3.dbr_ (for use on DBR 19)  | 2.13               |

Fabric 1.3 uses the 3.5.0.oss_ runtime, other Fabric runtimes may run on their equivalent OSS version.

Introduced in 0.2.0 is support for Spark Connect driven development, via the quality_api jar (shown above for 4.0.0 and 4.1.0), this includes Databricks Shared Compute support but requires [Session extensions](#using-the-sql-functions-on-spark-thrift-hive-servers).  

!!! info "0.1.3 Requires com.sparkutils.frameless for newer releases"
    Quality 0.1.3 uses [com.sparkutils.frameless](https://github.com/sparkutils/frameless) for the 3.5, 13.3 and 14.x releases together with the [shim project](https://github.com/sparkutils/shim), allowing quicker releases of Databricks runtime supports going forward.
    The two frameless code bases are not binary compatible and will require recompilation.
    This may revert to org.typelevel.frameless in the future.

## Sql functions vs column dsl

Similar to normal Spark functions there Quality's functions have sql variants to use with select / sql or expr() and the dsl variants built around Column.

You can use both the sql and dsl functions often without any other Quality runner usage, including lambdas.  To use the dsl functions, import quality.functions._, to use the sql functions you can either use the SparkExtension or the regsterXX functions available from the quality package.    

### Developing for a Databricks Runtime

As there are many compatibility issues that Quality works around between the various Spark runtimes and their Databricks equivalents you will need to use two different runtimes when you do local testing (and of course you _should_ do that):

```xml
<properties>
    <qualityVersion>0.2.0</qualityVersion>
    <qualityTestPrefix>4.1.0.oss_</qualityTestPrefix>
    <qualityDatabricksPrefix>18.3.dbr_</qualityDatabricksPrefix>
    <sparkShortVersion>4.1</sparkShortVersion>
    <scalaCompatVersion>2.13</scalaCompatVersion>    
</properties>

<dependencies>
    <dependency>
        <groupId>com.sparkutils.</groupId>
        <artifactId>quality_${qualityTestPrefix}${sparkShortVersion}_${scalaCompatVersion}</artifactId>
        <version>${qualityVersion}</version>
        <scope>test</scope>
    </dependency>
    <dependency>
        <groupId>com.sparkutils</groupId>
        <artifactId>quality_${qualityDatabricksPrefix}${sparkShortVersion}_${scalaCompatVersion}</artifactId>
        <version>${qualityVersion}</version>
        <scope>compile</scope>
    </dependency>
</dependencies>
```

That horrific looking "." on the test groupId is required to get Maven 3 to use different versions [many thanks for finding this Zheng](https://stackoverflow.com/a/67743309).

It's safe to assume better build tools like gradle / sbt do not need such hackery. 

The known combinations requiring this approach is below:

| Spark Version | sparkShortVersion | qualityTestPrefix | qualityDatabricksPrefix         | scalaCompatVersion |
|---------------|-------------------|-------------------|---------------------------------|--------------------|
| 3.3.2         | 3.3               | 3.3.2.oss_        | 12.2.dbr_                       | 2.12               | 
| 3.4.1         | 3.4               | 3.4.1.oss_        | 13.3.dbr_                       | 2.12               | 
| 3.5.0         | 3.5               | 3.5.0.oss_        | 14.3.dbr_                       | 2.12               | 
| 3.5.0         | 3.5               | 3.5.0.oss_        | 15.4.dbr_                       | 2.12               |
| 3.5.0         | 3.5               | 3.5.0.oss_        | 16.4.dbr_                       | 2.12               |
| 4.0.0         | 4.0               | 4.0.0.oss_        | 17.3.dbr_                       | 2.13               |
| 4.1.0         | 4.1               | 4.1.0.oss_        | 18.3.dbr_                       | 2.13               |
| 4.2.0         | 4.2               | 4.2.0.oss_        | 18.3.dbr_  (for use on DBR 19)  | 2.13               |

See [Connect](connect.md#how-to-build-applications-against-connect-with-an-extension) for quality_api based information (Spark 4 onwards).

## Using the SQL functions on Spark Thrift (Hive) servers

Using the configuration option:

```
spark.sql.extensions=com.sparkutils.quality.impl.extension.QualitySparkExtension
```

when starting your cluster, with the appropriate compatible Quality runtime jars - the test Shade jar can also be used -, will automatically register the additional SQL functions from Quality.

!!! note "Pure SQL only"    
    Lambdas, blooms and map's cannot be constructed via pure sql, so the functionality of these on Thrift/Hive servers is limited. 

### Query Optimisations

The Quality SparkExtension also provides query plan optimisers that re-write as_uuid and id_base64 usage when compared to strings.  This allows BI tools to use the results of view containing as_uuid or id_base64 strings in dashboards.  When the BI tool filters or selects on these strings passed down to the **same view**, the string is converted back into its underlying parts.  This allows for predicate pushdowns and other optimisations against the underlying parts instead of forcing conversions to string.

These two currently existing optimisations are applied to joins and filters against =, <=>, >, >=, <, <= and "in".

In order to use the query optimisations within normal job / calculator writing you must still register via spark.sql.extensions but you'll also be able to continue using the rest of the Quality functionality.  

The extension also enables the FunNRewrite optimisation (as of 0.1.3.1 and Spark 3.2 and higher) which expands user functions allowing sub expression elimination.

Optimisations may be disabled through the quality_disable_optimiser_rules spark configuration with comma separated fully qualified class names.

### Configuring on Databricks classic runtimes

In order to register the extensions on Databricks runtimes you need to additionally create a cluster init script much like:

```bash
#!/bin/bash

cp /dbfs/FileStore/quality_testshade_18.1.dbr_4.1_2.13-0.2.0.jar /databricks/jars/quality_testshade_18.1.dbr_4.1_2.13-0.2.0.jar
```

where the first path is your uploaded jar location.  You can create this script via a notebook on running cluster in the same workspace with throwaway code much like this:

```scala
val scriptName = "/dbfs/add_quality_plugin.sh"
val script = s"""
#!/bin/bash

cp /dbfs/FileStore/quality_testshade_18.1.dbr_4.1_2.13-0.2.0.jar /databricks/jars/quality_testshade_18.1.dbr_4.1_2.13-0.2.0.jar
"""
import java.io._

new File(scriptName).createNewFile
new PrintWriter(scriptName) {write(script); close}
```

You must still register the Spark config extension attribute, but also make sure the Init script has the same path as the file you created in the above snippet.

!!! important "Dos2Unix"
    If you are using Windows as your dev env, you will probably have to ensure your line endings are unix, so using git-portable and dos2unix before uploading your file if your are not generating it.

### Configuring on Databricks shared runtimes

Supported from DBR 17.3/18.x and Quality 0.2.0 only, you must enable init scripts in the UC metastore for a volume.  For example:

```bash
#!/bin/bash

cp /Volumes/databricks_ws/schema/jars/quality_testshade_18.1.dbr_4.1_2.13-0.2.0.jar /databricks/jars/quality_testshade_18.1-0.2.0.jar
```
