---
tags:
- basic
- getting started
- beginner
---

LTS' get explicit support, other interim versions may be supported as needed.

## Testing out Quality via Notebooks

You can use the appropriate runtime quality_testshade artefact jar (e.g. [DBR 18.3](https://s01.oss.sonatype.org/content/repositories/releases/com/sparkutils/quality_testshade_18.3.dbr_4.1_2.13/)) from maven to upload into your workspace / notebook env (or add via maven).  When using Databricks make sure to use the appropriate _Version.dbr builds.

Then, with cluster nodes of 64gb ram (only BigRules requires this, so 16gb is fine when just using the shades to experiment), using:

```scala
import com.sparkutils.qualityTests.QualityTestRunner
import com.sparkutils.testing.SparkTestUtils

// uncomment to disable connect test usage on runtimes that support it, like DBR 17.3
// System.setProperty("SPARKUTILS_DISABLE_CONNECT_TESTS","true")

// uncomment to disable classic test usage on runtimes that support connect, DBR 17.3
// a good use case is simulating a UC shared cluster (with init script / spark session extensions enabled) 
// when running on a classic cluster setup.
// On an actual UC Shared Compute cluster this is true by default (as the connect.SparkSession is provided) 
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

Ideally at the end of your runs you'll see - after up to an hour or so and some stdout - for example a run on DBR 17.3 provides:

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

!!! note "Databricks 17.x / 18.x"
    When running the test suite with an extension and classic cluster library several optimisation tests will fail (as the extension has already enabled them they cannot be disabled through the test suite).
    These tests (usually FunNRewrite based) can only be tested via classic clusters without extension.

!!! note "Databricks 18.x / UC introduces different delta behaviour"
    When running the test suite on 18.x the Delta extension tests will fail with spurious errors, these do not occur when using OSS or earlier DBRs.

## Running on Databricks Runtime 19

As of 0.2.0 Databricks 19 (noted as of release__19.x-snapshot-photon-scala2.13__databricks__19.2.4__affc832__32d3219__jenkins__59c4a83__format-3) crashes when trying to run tests and is unsupported.

## Running on Databricks Runtime 18 LTS

The following test combinations are supported as of 0.2.0:

| Compute Type   | Cluster Library                      | Extension                    | Connect Via quality_api | Full Pre 0.2.0 Functionality | QualityTestRunner Test Count                | SPARKUTILS_DISABLE_CLASSIC_TESTS (default false) | SPARKUTILS_DISABLE_CONNECT_TESTS (default false) | Time Taken Standard_D8ds_v5 32gb 8 cores 2 executors (m) |
|----------------|--------------------------------------|------------------------------|-------------------------|------------------------------|---------------------------------------------|--------------------------------------------------|--------------------------------------------------|---------------------------------------------------------:|
| Non Shared     | quality_testshade_18.3.dbr           |                              |                         | :octicons-checkbox-24:       | > 500                                       |                                                  | true                                             |                                                       30 |
| Non Shared     | quality_testshade_18.3.dbr           | quality_testshade_18.3.dbr   | :octicons-checkbox-24:  | :octicons-checkbox-24:       | > 500 tests, default < 180 tests in Connect |                                                  |                                                  |                                                       60 |
| Shared Compute | quality_connect_testshade_18.3.dbr   | quality_testshade_18.3.dbr   | :octicons-checkbox-24:  |                              | < 180                                       | true                                             |                                                  |                                                       17 |
| Shared Compute | quality_connect_testshade_4.1.0.oss  | quality_testshade_18.3.dbr   | :octicons-checkbox-24:  |                              | < 180                                       | true                                             |                                                  |                                                       17 |
| Shared Compute | quality_api_18.3.dbr                 | quality_18.3.dbr             | :octicons-checkbox-24:  |                              | :octicons-circle-slash-24:                  |                                                  |                                                  |                                                          |
| Shared Compute | quality_api_4.1.0.oss                | quality_18.3.dbr             | :octicons-checkbox-24:  |                              | :octicons-circle-slash-24:                  |                                                  |                                                  |                                                          |

Databricks 18 has been tested as of release__18.3.x-snapshot-photon-scala2.13__databricks__18.3.3__bfd1bbf__b0356d3__jenkins__bc102bd__format-3 (search for spark.databricks.clusterUsageTags.sparkImageLabel on the environment spark properties page for the exact version your cluster uses)

## Running on Databricks Runtime 17.3 LTS

Supported as of 0.1.3.1.  Only 0.2.0 supports connect and shared cluster, with extension, usage.

17.3, in addition to Spark 4 usage, introduced a binary incompatible change to NamedExpressions not present in the OSS codebase.

!!! info "Non-Shared Connect"
    Using quality_testshade on both client and extension allows mixing modes.  To use connect on the test cases leverage:
    
    ```scala
    SparkSession.builder.config("spark.api.mode", "connect").getOrCreate()
    ```
    
    from the testing 17.3 specific build.  For any Fabric (future) / OSS similar approaches using the quality_testshade_4.0.0.oss developers will need to set:

    ```scala
    System.setProperty("SPARKUTILS_TESTING_USE_LOCAL_CONNECT", "true")
    ```

    before running any tests to stop an attempt to spawned jvm's (which is never needed with Databricks).

!!! note "Shading/Uber jar"
    The last two combinations require users to correctly manage the shading and, as such, do not include the test jars or any of the jarjar handling.

    See [connect](connect.md) for details on how the jars / libraries can be built, correctly shaded and more importantly how the server side can now evolve independently of your client application code.

### Using Lakeguard / Shared Compute clusters with 0.2.0

In order to use Shared Compute clusters you must register spark [session extensions](index.md#configuring-on-databricks-shared-runtimes) for the server side quality (to be run on the driver and executor nodes).

As this mode is purely connect, no ClassicOnly functions will be usable. 

#### Which shade to use for exploration or testing?

You can use either the quality_connect_testshade or quality_testshade to test or experiment in workbooks in this setup.
The quality_connect_testshade only packages the quality_api so runs fully only via Spark 4 Connect apis, moreover - as it doesn't require server side code you can also use the oss shade.

When using the connect_testshade jar the number of tests run is smaller (only 118 pure connect tests are run) but you should see similar output to:

```
Quality - starting test batch 0
Run starting. Expected test count is: 118
AggregatesTest:
- mapTest (10 seconds, 151 milliseconds)
- multiGroups (6 seconds, 610 milliseconds)
- testFlattenResults (1 second, 809 milliseconds)
- testSalience (1 second, 817 milliseconds)
- testDebug (293 milliseconds)
...
Run completed in 4 minutes, 22 seconds.
Total number of tests run: 118
Suites: completed 10, aborted 0
Tests: succeeded 118, failed 0, canceled 0, ignored 0, pending 0
All tests passed.
Quality - gc'ing after finishing test batch 0
Quality - starting test batch 1
Run starting. Expected test count is: 53
...
Run completed in 1 minute, 28 seconds.
Total number of tests run: 53
Suites: completed 6, aborted 0
Tests: succeeded 53, failed 0, canceled 0, ignored 0, pending 0
All tests passed.
Quality - gc'ing after finishing test batch 1
all Quality test batches completed
```

#### Known Issues

* Logging INFO with map operations, despite these being implemented by Quality, you can ignore these
> INFO Log4jUsageLogger: sparkThrowable=1.0, tags=List(errorClass=UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE), blob=null
> INFO Log4jUsageLogger: sparkThrowable=1.0, tags=List(errorClass=DATATYPE_MISMATCH.INVALID_ORDERING_TYPE), blob=null
* Any use of Spark Classic / catalyst internals on a shared cluster can trigger very wierd issues such as Verify or ImplementationChanged Errors.
* A number of stacks will seemingly flash in the output window after starting a cell in the notebooks, this seems unrelated to Quality.

## Running on Databricks Runtime 12.2 LTS

DBR 12.2 backports at least [SPARK-41049](https://issues.apache.org/jira/browse/SPARK-41049) from 3.4 so the base build is closer to 3.4 than the advertised 3.3.2.  Building/Testing against 3.3.0 is the preferred approach for maximum compatibility.

## Running on Databricks Runtime 13.3 LTS

13.3 backports yet more 3.5 so the 13.3.dbr build must be used.

## Running on Databricks Runtime 14.3 LTS

14.3, in addition to the 14.2 StaticInvoke and ResolveReferences changes also implements a new VarianceChecker that requires a new 14.3.dbr runtime.

## Running on Databricks Runtime 15.4 LTS

Supported as of 0.1.3.1.

15.4 LTS now requires its own runtime if you are using rng functions as Databricks introduced a breaking change in optimisation of Nondeterministic functions (which relies on a newly introduced Expression.nonVolatile field not present in OSS Spark)

## Running on Databricks Runtime 16.4 LTS

Supported as of 0.1.3.1.

16.3 Introduced a number of API changes, Stream is returned in some unexpected forceInterpreted cases,  and UnresolvedFunction gets a new param.  

## Running on Databricks Runtime 17.3 / 18.3 / 18 LTS

Support via 0.2.0 and is tested on latest 17.3 and 18 (via 18.3.dbr - build 18.3.3 observed via spark.databricks.clusterUsageTags.sparkImageLabel).

Databricks 18 backported Spark 4.2.0's UnresolvedFunction interface and, as such, requires its own runtime support.

## Running on Databricks Runtime 19

Support via 0.2.0 and the 18.3.dbr release (tested against 19.1.3 release__19.x-snapshot-photon-scala2.13__databricks__19.1.3__132a64e__553b6cb__jenkins__d71fd8f__format-3),
DBFS however is no longer usable as such, per #141, bucketed large bloom filters no longer work.
