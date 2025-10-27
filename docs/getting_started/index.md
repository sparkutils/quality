---
tags:
   - basic
   - getting started
   - beginner
---

## Migrating from 0.0.3 to 0.1.0

The quality package has been trimmed down to common functionality only.  DSL / Column based functions and types have moved to specific packages similar to implicits:

```scala
import com.sparkutils.quality._
import functions._
import types._
import implicits._
```  

The functions package aims to have an equivalent column dsl function for each bit of sql based functionality.  The notable exception to this is the lambda, callFun and _() functions, for which you are better off using your languages normal support for abstraction.  A number of the functions have been, due to naming choice, deprecated they will be removed in 0.2.0.   

## Building The Library

* fork, 
* use the Scala dev environment of your choice,
* or build directly using Maven

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
quality_4.0.0.oss_4.0_2.13-0.1.3.1.jar
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

| Spark Version | sparkShortVersion | qualityRuntime | scalaCompatVersion |
|---------------|-------------------|----------------|--------------------|
| 2.4.6         | 2.4               |                | 2.11               | 
| 3.0.3         | 3.0               |                | 2.12               | 
| 3.1.3         | 3.1               |                | 2.12               | 
| 3.1.3         | 3.1               | 9.1.dbr_       | 2.12               | 
| 3.2.0         | 3.2               |                | 2.12               | 
| 3.2.1         | 3.2               | 3.2.1.oss_     | 2.12               | 
| 3.2.1         | 3.2               | 10.4.dbr_      | 2.12               | 
| 3.3.2         | 3.3               | 3.3.2.oss_     | 2.12               | 
| 3.3.2         | 3.3               | 11.3.dbr_      | 2.12               |
| 3.3.2         | 3.3               | 12.2.dbr_      | 2.12               |
| 3.3.2         | 3.3               | 13.1.dbr_      | 2.12               |
| 3.4.1         | 3.4               | 3.4.1.oss_     | 2.12               |
| 3.4.1         | 3.4               | 13.1.dbr_      | 2.12               |
| 3.4.1         | 3.4               | 13.3.dbr_      | 2.12               |
| 3.5.0         | 3.5               | 3.5.0.oss_     | 2.12               |
| 3.5.0         | 3.5               | 14.0.dbr_      | 2.12               |
| 3.5.0         | 3.5               | 14.3.dbr_      | 2.12               |
| 3.5.0         | 3.5               | 15.4.dbr_      | 2.12               |
| 3.5.0         | 3.5               | 16.4.dbr_      | 2.12               |
| 4.0.0         | 4.0               | 4.0.0.oss_     | 2.13               |
| 4.0.0         | 4.0               | 17.3.dbr_      | 2.13               |

Fabric 1.3 uses the 3.5.0.oss_ runtime, other Fabric runtimes may run on their equivalent OSS version.

2.4, 9.1.dbr, 10.4.dbr and 11.3.dbr support is deprecated and will be removed in 0.1.4 version.  3.1.2 support is replaced by 3.1.3 due to interpreted encoder issues. 

!!! note "Databricks 13.x support"
    13.0 also works on the 12.2.dbr_ build as of 10th May 2023, despite the Spark version difference.
    13.1 requires its own version as it backports 3.5 functionality.  The 13.1.dbr quality runtime build also works on 13.2 DBR. 
    13.3 LTS has its own runtime

!!! warning "Databricks 14.x support"
    Due to back-porting of SPARK-44913 frameless 0.16.0 (the 3.5.0 release) is not binary compatible with 14.2 and above which has back-ported this 4.0 interface change.
    Similarly, 4.0 / 14.2 introduces a change in resolution so a new runtime version is required upon a potential fix for 44913 in frameless.
    As such 14.3 has its own runtime

!!! warning "0.1.3 Requires com.sparkutils.frameless for newer releases"
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
    <qualityVersion>0.1.3.1</qualityVersion>
    <qualityTestPrefix>4.0.0.oss_</qualityTestPrefix>
    <qualityDatabricksPrefix>17.3.dbr_</qualityDatabricksPrefix>
    <sparkShortVersion>4.0</sparkShortVersion>
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

| Spark Version | sparkShortVersion | qualityTestPrefix | qualityDatabricksPrefix | scalaCompatVersion |
|---------------|-------------------|-------------------|-------------------------|--------------------|
| 3.2.1         | 3.2               | 3.2.1.oss_        | 10.4.dbr_               | 2.12               | 
| 3.3.0         | 3.3               | 3.3.0.oss_        | 11.3.dbr_               | 2.12               | 
| 3.3.2         | 3.3               | 3.3.2.oss_        | 12.2.dbr_               | 2.12               | 
| 3.4.1         | 3.4               | 3.4.1.oss_        | 13.1.dbr_               | 2.12               | 
| 3.5.0         | 3.5               | 3.5.0.oss_        | 14.0.dbr_               | 2.12               | 
| 3.5.0         | 3.5               | 3.5.0.oss_        | 14.3.dbr_               | 2.12               | 
| 3.5.0         | 3.5               | 3.5.0.oss_        | 15.4.dbr_               | 2.12               |
| 3.5.0         | 3.5               | 3.5.0.oss_        | 16.4.dbr_               | 2.12               |
| 4.0.0         | 4.0               | 4.0.0.oss_        | 17.3.dbr_               | 2.13               |

## Using the SQL functions on Spark Thrift (Hive) servers

Using the configuration option:

```
spark.sql.extensions=com.sparkutils.quality.impl.extension.QualitySparkExtension
```

when starting your cluster, with the appropriate compatible Quality runtime jars - the test Shade jar can also be used -, will automatically register the additional SQL functions from Quality.

!!! note "Spark 2.4 runtimes are not supported"
    2.4 is not supported as Spark doesn't provide for SQL extensions in this version.
      
!!! note "Pure SQL only"    
    Lambdas, blooms and map's cannot be constructed via pure sql, so the functionality of these on Thrift/Hive servers is limited. 

### Query Optimisations

The Quality SparkExtension also provides query plan optimisers that re-write as_uuid and id_base64 usage when compared to strings.  This allows BI tools to use the results of view containing as_uuid or id_base64 strings in dashboards.  When the BI tool filters or selects on these strings passed down to the **same view**, the string is converted back into its underlying parts.  This allows for predicate pushdowns and other optimisations against the underlying parts instead of forcing conversions to string.

These two currently existing optimisations are applied to joins and filters against =, <=>, >, >=, <, <= and "in".

In order to use the query optimisations within normal job / calculator writing you must still register via spark.sql.extensions but you'll also be able to continue using the rest of the Quality functionality.  

The extension also enables the FunNRewrite optimisation (as of 0.1.3.1 and Spark 3.2 and higher) which expands user functions allowing sub expression elimination.

### Configuring on Databricks runtimes

In order to register the extensions on Databricks runtimes you need to additionally create a cluster init script much like:

```bash
#!/bin/bash

cp /dbfs/FileStore/XXXX-quality_testshade_12_2_ver.jar /databricks/jars/quality_testshade_12_2_ver.jar
```

where the first path is your uploaded jar location.  You can create this script via a notebook on running cluster in the same workspace with throwaway code much like this:

```scala
val scriptName = "/dbfs/add_quality_plugin.sh"
val script = s"""
#!/bin/bash

cp /dbfs/FileStore/XXXX-quality_testshade_12_2_ver.jar /databricks/jars/quality_testshade_12_2_ver.jar
"""
import java.io._

new File(scriptName).createNewFile
new PrintWriter(scriptName) {write(script); close}
```

You must still register the Spark config extension attribute, but also make sure the Init script has the same path as the file you created in the above snippet.

## 2.4 Support requires 2.4.6 or Janino 3.0.16

Due to [Janino #90](https://github.com/janino-compiler/janino/issues/90) using 2.4.5 directly will bring in 3.0.9 janino which can cause VerifyErrors, use 2.4.6 if you can't use a 3.x Spark.

