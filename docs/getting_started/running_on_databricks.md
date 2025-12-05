---
tags:
- basic
- getting started
- beginner
---

LTS' get explicit support, other interim versions may be supported as needed.

## Testing out Quality via Notebooks

You can use the appropriate runtime quality_testshade artefact jar (e.g. [DBR 17.3](https://s01.oss.sonatype.org/content/repositories/releases/com/sparkutils/quality_testshade_17.3.dbr_4.0_2.13/)) from maven to upload into your workspace / notebook env (or add via maven).  When using Databricks make sure to use the appropriate _Version.dbr builds.

Then using:

```scala
import com.sparkutils.qualityTests.QualityTestRunner
import com.sparkutils.testing.SparkTestUtils

// uncomment to disable connect test usage on runtimes that support it, like DBR 17.3
// System.setProperty("SPARKUTILS_DISABLE_CONNECT_TESTS","true")

// uncomment to disable classic test usage on runtimes that support connect, DBR 17.3
// a good use case is when using a UC shared cluster with init script / spark session extensions enabled, where
// classic doesn't actually exist
// System.setProperty("SPARKUTILS_DISABLE_CLASSIC_TESTS","true")

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

## Running on Databricks Runtime 17.3 LTS

Supported as of 0.1.3.1.

17.3, in addition to Spark 4 usage, introduced a binary incompatible change to NamedExpressions not present in the OSS codebase.

The following test combinations are supported:

| Compute Type   | Cluster Library                     | Extension | Scope of Testing / Usage Possible                                                                                                                                     |
|----------------|-------------------------------------|-----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------| 
| Non Shared     | quality_testshade_17.3              | none                          | All of classic Quality, no connect (>400 tests)                                                                                                                       |
| Non Shared     | quality_testshade_17.3              | quality_testshade_17.3  | All of Quality, classic and spark.api.mode connect (>400 tests default, < 120 tests when SPARKUTILS_DISABLE_CLASSIC_TESTS is set to true)                             |
| Shared Compute | quality_testshade_17.3              | quality_testshade_17.3  | Connect supported Quality only (no Blooms, resolve, overriden registration, classic functions etc.) (< 120 tests - SPARKUTILS_DISABLE_CLASSIC_TESTS defaults to true) |
| Shared Compute | quality_connect_testshade_17.3      | quality_testshade_17.3  | Connect supported Quality only (no Blooms, resolve, overriden registration, classic functions etc.) (< 120 tests - SPARKUTILS_DISABLE_CLASSIC_TESTS defaults to true) |
| Shared Compute | quality_connect_testshade_4.0.0.oss | quality_testshade_17.3  | Connect supported Quality only (no Blooms, resolve, overriden registration, classic functions etc.) (< 120 tests - SPARKUTILS_DISABLE_CLASSIC_TESTS defaults to true) |




### Using Lakeguard / Shared clusters with 0.2.0

In order to use shared clusters you must still use quality libraries for your client code, but you must also register spark [session extensions](index.md#configuring-on-databricks-shared-runtimes).

As this mode is purely connect, no ClassicOnly functions will be usable, so if running the test pack - ensure you use:

```scala
System.setProperty("SPARKUTILS_DISABLE_CLASSIC_TESTS","true")
```

#### Which shade to use for exploration or testing?

You can use either the quality_connect_testshade or quality_testshade to test or experiment in workbooks in this setup.
The quality_connect_testshade only packages the quality_api so runs fully only via Spark 4 Connect apis, moreover - as it doesn't require server side code you can also use the oss shade.

When using the connect_testshade jar the number of tests run is smaller (only 111 pure connect tests are run) but you should see similar output to:

```
Quality - starting test batch 0
Run starting. Expected test count is: 111
AggregatesTest:
- mapTest (10 seconds, 151 milliseconds)
- multiGroups (6 seconds, 610 milliseconds)
- testFlattenResults (1 second, 809 milliseconds)
- testSalience (1 second, 817 milliseconds)
- testDebug (293 milliseconds)
...
Run completed in 2 minutes, 27 seconds.
Total number of tests run: 111
Suites: completed 10, aborted 0
Tests: succeeded 111, failed 0, canceled 0, ignored 0, pending 0
All tests passed.
Quality - gc'ing after finishing test batch 0
all Quality test batches completed
```

#### Known Issues

* Logging INFO with map operations, despite these being implemented by Quality, you can ignore these
> INFO Log4jUsageLogger: sparkThrowable=1.0, tags=List(errorClass=UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE), blob=null
> INFO Log4jUsageLogger: sparkThrowable=1.0, tags=List(errorClass=DATATYPE_MISMATCH.INVALID_ORDERING_TYPE), blob=null
* Any use of Spark Classic / catalyst internals on a shared cluster can trigger very wierd issues such as Verify or ImplementationChanged Errors.  The Quality test pack can be used as a guide here (a connect safe library is pending).
* A number of stacks will seemingly show before running your code in the notebooks, this seems unrelated to Quality.

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
