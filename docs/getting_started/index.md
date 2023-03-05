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

As with any local Spark development, in order to run the tests you must have the vcredist 2010 and winutils packages installed, for Spark 2.4.6 and 3.0 it can be downloaded from [here]().

If you are using 3.1.2 or 3.2 download both the dll and exe from [here](https://github.com/cdarlint/winutils/tree/master/hadoop-3.2.0/bin) and ensure that not only is the HADOOP_HOME defined but that the bin directory within it is on the PATH, you may need to restart Intellij.

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
    <groupId>com.sparkutils.quality</groupId>
    <artifactId>quality_${qualityRuntime}${sparkShortVersion}_${scalaCompatVersion}</artifactId>
    <version>${qualityVersion}</version>
</dependency>
```

The full list of supported runtimes is below:

| Spark Version | sparkShortVersion | qualityRuntime | scalaCompatVersion |
| - | - | - | - |
| 2.4.6 | 2.4 | | 2.11 | 
| 3.0.3 | 3.0 | | 2.12 | 
| 3.1.2 | 3.1 | | 2.12 | 
| 3.1.2 | 3.1 | 9.1.dbr_ | 2.12 | 
| 3.2.0 | 3.2 | | 2.12 | 
| 3.2.0 | 3.2 | 10.2.dbr_ | 2.12 | 
| 3.2.1 | 3.2 | 3.2.1.oss_ | 2.12 | 
| 3.2.1 | 3.2 | 10.4.dbr_ | 2.12 | 
| 3.3.0 | 3.3 | 3.3.0.oss_ | 2.12 | 
| 3.3.0 | 3.3 | 11.0.dbr_ | 2.12 | 


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
        <groupId>com.sparkutils.quality.</groupId>
        <artifactId>quality_${qualityTestPrefix}${sparkShortVersion}_${scalaCompatVersion}</artifactId>
        <version>${qualityVersion}</version>
        <scope>test</scope>
    </dependency>
    <dependency>
        <groupId>com.sparkutils.quality</groupId>
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
| 3.3.0 | 3.3 | 3.3.0.oss_ | 11.0.dbr_ | 2.12 | 


## 2.4 Support requires 2.4.6 or Janino 3.0.16

Due to [Janino #90](https://github.com/janino-compiler/janino/issues/90) using 2.4.5 directly will bring in 3.0.9 janino which can cause VerifyErrors, use 2.4.6 if you can't use a 3.x Spark.

