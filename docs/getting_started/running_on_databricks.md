---
tags:
- basic
- getting started
- beginner
---

The aim is to have explicit support for LTS', other interim versions may be supported as needed.

## Running on Databricks Runtime 12.2 LTS

DBR 12.2 backports at least [SPARK-41049](https://issues.apache.org/jira/browse/SPARK-41049) from 3.4 so the base build is closer to 3.4 than the advertised 3.3.2.  Building/Testing against 3.3.0 is the preferred approach for maximum compatibility. 

## Running on Databricks Runtime 13.0

As of 6th June 2023 0.0.2 run against the 12.2.dbr LTS build also works on 13.0.

## Running on Databricks Runtime 13.1/13.2
  
13.1 backports a number of 3.5 oss changes, the 13.1.dbr build must be used.  The 13.1.dbr build is also successfully tested against 13.2 DBR.    

!!! WARN "The 13.1/2 runtimes, given the LTS version, are deprecated and will be removed in 0.1.4."

## Running on Databricks Runtime 13.3 LTS

13.3 backports yet more 3.5 so the 13.3.dbr build must be used.

## Running on Databricks Runtime 14.0/14.1
  
14.0 and 14.1 can be used with the 14.0.dbr runtime, 14.2 however is not compatible, it back-ports two changes that render Quality 0.1.3 impossible to run:

1. 44913 - StaticInvoke has changed breaking frameless binary compatibility
2. ResolveReferences now takes catalogue as a parameter

!!! WARN "The 14.0/1 runtimes, given the LTS version, are deprecated and will be removed in 0.1.4."

## Running on Databricks Runtime 14.3 LTS

14.3, in addition to the 14.2 StaticInvoke and ResolveReferences changes also implements a new VarianceChecker that requires a new 14.3.dbr runtime.

## Running on Databricks Runtime 15.4 LTS

Supported as of 0.1.3.1.

15.4 LTS now requires its own runtime if you are using rng functions as Databricks introduced a breaking change in optimisation of Nondeterministic functions (which relies on a newly introduced Expression.nonVolatile field not present in OSS Spark)

## Running on Databricks Runtime 16.4 LTS

Supported as of 0.1.3.1.

16.3 Introduced a number of API changes, Stream is returned in some unexpected forceInterpreted cases,  and UnresolvedFunction gets a new param.  

## Running on Databricks Runtime 17.3 LTS

Supported as of 0.1.3.1.

17.3, in addition to Spark 4 usage, introduced a binary incompatible change to NamedExpressions not present in the OSS codebase. 

## Testing out Quality via Notebooks

You can use the appropriate runtime quality_testshade artefact jar (e.g. [DBR 11.3](https://s01.oss.sonatype.org/content/repositories/releases/com/sparkutils/quality_testshade_11.3.dbr_3.3_2.12/)) from maven to upload into your workspace / notebook env (or add via maven).  When using Databricks make sure to use the appropriate _Version.dbr builds.

Then using:

```scala
import com.sparkutils.qualityTests.QualityTestRunner
import com.sparkutils.testing.SparkTestUtils

// uncomment to disable connect test usage on DBR 17.3
// System.setProperty("SPARKUTILS_DISABLE_CONNECT_TESTS","true")

// for running on azure set the configuration for both classic and connect client
val keyMap = Map(s"fs.azure.account.key.${srv_path}${dfs}" -> accountKey)
SparkTestUtils.setRuntimeConnectClientConfig(keyMap)
SparkTestUtils.setRuntimeClassicConfig(keyMap)

val root_path = loc
SparkTestUtils.setPath(root_path+"/qualityTests")
QualityTestRunner.test()
```

in your cell will run through all of the test suite used when building Quality.

Ideally at the end of your runs you'll see - after 10 minutes or so and some stdout - for example a run on DBR 17.3 provides:

```
Quality - starting test batch 0
Run starting. Expected test count is: 183
....
Run completed in 2 minutes, 49 seconds.
Total number of tests run: 183
Suites: completed 10, aborted 0
Tests: succeeded 183, failed 0, canceled 0, ignored 0, pending 0
All tests passed.
projectName - gc'ing after finishing test batch 0
Quality - starting test batch 1
....
Run completed in 1 minute.
Total number of tests run: 158
Suites: completed 10, aborted 0
Tests: succeeded 158, failed 0, canceled 0, ignored 1, pending 0
All tests passed.
....
Run completed in 1 minute, 56 seconds.
Total number of tests run: 106
Suites: completed 10, aborted 0
Tests: succeeded 106, failed 0, canceled 0, ignored 0, pending 0
All tests passed.
Quality - gc'ing after finishing test batch 2
all Quality test batches completed
```
