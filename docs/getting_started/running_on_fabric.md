---
tags:
- basic
- getting started
- beginner
---

Fabric support has been added since 0.1.3.1 and, at time of the 2.0 runtime, mostly follows the OSS Spark codebase.  Other OSS stack to Synapse/Fabric runtimes may similarly "just" work. 

## Running on Fabric 2

Use the OSS 4.1 build and testShades.  Please note:

1. there is no connect support so
    ```scala
     System.setProperty("SPARKUTILS_DISABLE_CONNECT_TESTS","true")
    ```
    must be used
2. that push down filters for the optimisations ExtensionTests do not work when using the Quality spark.sql.sqlextensions
3. delta issues with merge schema, as with databricks, fails on the ExtensionTests,
4. compare tests fail as the message is custom:
    ```
    BaseFunctionalityTest:
    
    - testCompareWithArraysOrderingAndReverse *** FAILED *** (90 milliseconds)
      "Is assumed to fail as spark doesn't order maps" did not contain "data type mismatch" (BaseFunctionalityTest.scala:540)
      quality.org.scalatest.exceptions.TestFailedException:
    ```
5. Bloom tests fail:
    ```
    BloomTests:

    - verifyCompilationSpark *** FAILED *** (100 milliseconds)
     java.lang.IllegalArgumentException: Unknown BloomFilter version: 0
      at org.apache.spark.util.sketch.BloomFilter.readFrom(BloomFilter.java:205)
      at com.sparkutils.quality.impl.bloom.SparkBloomFilterSerializer$.fromType(model.scala:94)
    ```

## Running on Fabric 1.3

Use the OSS 3.5.0 build and testShades.

## Testing out Quality via Notebooks

This behaves the same way as [per Databricks](running_on_databricks.md#testing-out-quality-via-notebooks) with one notable exception, System.out is not redirected so you also need:

```scala
// in case it's needed again 
val ogSysOut = System.out
System.setOut(Console.out)
```

before running tests to see test progress.