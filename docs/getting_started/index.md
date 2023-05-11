---
tags:
   - basic
   - getting started
   - beginner
---

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

As with any local Spark development, in order to run the tests you must have the vcredist 2010 and winutils packages installed, for Spark 2.4.6 and 3.0 it can be downloaded from [here](https://github.com/cdarlint/winutils/blob/master/hadoop-2.7.7/bin/winutils.exe).

If you are using 3.1.3 or 3.2 download both the dll and exe from [here](https://github.com/cdarlint/winutils/tree/master/hadoop-3.2.0/bin) and ensure that not only is the HADOOP_HOME defined but that the bin directory within it is on the PATH, you may need to restart Intellij.

Also ensure only the correct target Maven profile and source directories are enabled in your IDE of choice. 

The performance tests are not automated and must be manually run when needed.

## Build tool dependencies

Quality is cross compiled for different versions of Spark, Scala _and_ runtimes such as Databricks.  The format for artefacts is:

```
quality_RUNTIME_SPARKCOMPATVERSION_SCALACOMPATVERSION-VERSION.jar
```

e.g.

```
quality_3.3.0.oss_3.3_2.12-0.7.0-SNAPSHOT.jar
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
|---------------| - | - | - |
| 2.4.6         | 2.4 | | 2.11 | 
| 3.0.3         | 3.0 | | 2.12 | 
| 3.1.3         | 3.1 | | 2.12 | 
| 3.1.3         | 3.1 | 9.1.dbr_ | 2.12 | 
| 3.2.0         | 3.2 | | 2.12 | 
| 3.2.1         | 3.2 | 3.2.1.oss_ | 2.12 | 
| 3.2.1         | 3.2 | 10.4.dbr_ | 2.12 | 
| 3.3.0         | 3.3 | 3.3.0.oss_ | 2.12 | 
| 3.3.0         | 3.3 | 11.3.dbr_ | 2.12 |
| 3.3.0         | 3.3 | 12.2.dbr_ | 2.12 |
| 3.4.0         | 3.4 | | 2.12 |

2.4 support is deprecated and will be removed in a future version.  3.1.2 support is replaced by 3.1.3 due to interpreted encoder issues. 

!!! note "Databricks 12.2 is experimental - pending Frameless 3.4 support"
    12.2 LTS is a mix of 3.3.0 and 3.4.0, as such until Frameless supports 3.4 [see here](https://github.com/typelevel/frameless/issues/698).  This _should_ not affect the sql function extensions.
    13.0 also works on the 12.2.dbr_ build as of 10th May 2023.

### Developing for a Databricks Runtime

As there are many compatibility issues that Quality works around between the various Spark runtimes and their Databricks equivalents you will need to use two different runtimes when you do local testing (and of course you _should_ do that):

```xml
<properties>
    <qualityVersion>0.7.0-SNAPSHOT</qualityVersion>
    <qualityTestPrefix>3.2.1.oss_</qualityTestPrefix>
    <qualityDatabricksPrefix>10.4.dbr_</qualityDatabricksPrefix>
    <sparkShortVersion>3.2</sparkShortVersion>
    <scalaCompatVersion>2.12</scalaCompatVersion>    
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
| - | - | - | - | - |
| 3.2.1 | 3.2 | 3.2.1.oss_ | 10.4.dbr_ | 2.12 | 
| 3.3.0 | 3.3 | 3.3.0.oss_ | 11.3.dbr_ | 2.12 | 
| 3.3.0 | 3.3 | 3.3.0.oss_ | 12.2.dbr_ | 2.12 | 

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

