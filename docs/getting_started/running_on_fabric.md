---
tags:
- basic
- getting started
- beginner
---

Fabric support has been added since 0.1.3.1 and, at time of the 1.3 runtime, follows the OSS Spark codebase.  Other OSS stack to Synapse/Fabric runtimes may similarly "just" work. 

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