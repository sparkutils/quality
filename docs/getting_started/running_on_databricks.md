---
tags:
- basic
- getting started
- beginner
---

The aim is to have explicit support for LTS', other interim versions may be supported as needed.

## Running 3.1 builds on Databricks Runtime 9.1 LTS

Use the 9.1.dbr build / profile, the artefact name will also end with _9.1.dbr.  OSS 3.1 do not need to worry about this and should not use this profile.

Databricks has back-ported TreePattern including the final nodePatterns in HigherOrderFunction and 3.2's Conf class.  As such very old versions of non-opensource Quality (<=0.5.0) will fail with AbstractMethodError's when lambda's are used are 9.1 as the OSS binary version of HigherOrderFunction does not have nodePattern.  Similarly, the quality_testshade jar must use the 9.1.dbr version due to Conf changes.

The 9.1.dbr build class files are built on the fake TreePattern and HigherOrderFunction present in the 9.1.dbr-scala source directory, they are however removed in the jar.

ResolveTableValuedFunctions and ResolveCreateNamedStruct are removed from resolveWith as they are binary incompatible with OSS.  This does not seem to effect building namedstructs using resolveWith.

## Running 3.2.1 builds on Databricks Runtime 10.4

Use the 10.4.dbr build / profile, the artefact name will also end with _10.4.dbr.

DBR 10.4 backports canonicalisation changes which allow Quality and any other code using explode and arrays to functionally run.  Performance is still known to be affected.  These fixes are not present in the 3.2.1 OSS release, although performance improvements may be back-ported.

ResolveTables, ResolveAlterTableCommands and ResolveHigherOrderFunctions are removed from resolveWith as they are binary incompatible with OSS.

!!! info "Only 10.4 LTS is supported"
    10.2 version support was removed in 0.0.1

## Running 3.3.0 builds on Databricks Runtime 11.3 LTS

Use the 11.3.dbr build / profile, the artefact name will also end with _11.3.dbr.  Due to a backport of [SPARK-39316](https://issues.apache.org/jira/browse/SPARK-39316) only 11.3 LTS is supported (although likely 11.2 will also run), this changed the result type of Add causing incorrect aggregation precision via aggExpr (Sum and Average stopped using Add for this reason).

## Running on Databricks Runtime 12.2 LTS

DBR 12.2 backports at least [SPARK-41049](https://issues.apache.org/jira/browse/SPARK-41049) from 3.4 so the base build is closer to 3.4 than the advertised 3.3.2.  Building/Testing against 3.3.0 is the preferred approach for maximum compatibility. 

## Running on Databricks Runtime 13.0

As of 6th June 2023 0.0.2 run against the 12.2.dbr LTS build also works on 13.0.

## Running on Databricks Runtime 13.1
  
13.1 backports a number of 3.5 oss changes, the 13.1.dbr build must be used - as of 25th May 2023.    

## Testing out Quality via Notebooks

You can use the appropriate runtime quality_testshade artefact jar (e.g. [DBR 11.3](https://s01.oss.sonatype.org/content/repositories/releases/com/sparkutils/quality_testshade_11.3.dbr_3.3_2.12/)) from maven to upload into your workspace / notebook env (or add via maven).  When using Databricks make sure to use the appropriate _Version.dbr builds.

Then using:

```scala
import com.sparkutils.quality.tests.TestSuite
import com.sparkutils.qualityTests.SparkTestUtils

SparkTestUtils.setPath("path_where_test_files_should_be_generated")
TestSuite.runTests
```

in your cell will run through all of the test suite used when building Quality.

In Databricks notebooks you can set the path up via:

```scala
val fileLoc = "/dbfs/databricks/quality_test"
SparkTestUtils.setPath(fileLoc)
```

Ideally at the end of your runs you'll see - after 10 minutes or so and some stdout - for example a run on DBR 13.1 provides:

```
Time: 633.686

OK (402 tests)

Finished. Result: Failures: 0. Ignored: 0. Tests run: 402. Time: 633686ms.
import com.sparkutils.quality.tests.TestSuite
import com.sparkutils.qualityTests.SparkTestUtils
fileLoc: String = /dbfs/databricks/quality_test
```
