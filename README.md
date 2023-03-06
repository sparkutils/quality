# Quality

Run high performance complex Data Quality and data processing rules using simple SQL in a batch or streaming Spark application at scale.

Write rules using simple SQL or create re-usable functions via SQL Lambdas - your rules are just versioned data, store them wherever convenient, use them by simply defining a column.

Rules are evaluated lazily during Spark actions, such as writing a row, with results saved in a single predictable and extensible column.

The documentation site https://sparkutils.github.io/quality/ breaks down the reason for Quality's existence, and it's usage.

## What's it written in?

Scala with sprinklings of java for WholeStageCodeGen optimisations.

## How do I use / build it?

For oss with Spark 3.3.0 use properties:

```xml
<properties>
    <qualityRuntime>3.3.0.oss_</qualityRuntime>
    <scalaCompatVersion>2.12</scalaCompatVersion>
    <sparkShortVersion>3.3</sparkShortVersion>
    <qualityVersion>0.0.1-SNAPSHOT</qualityVersion>
</properties>
```

with dependency:

```xml
<dependency>
    <groupId>com.sparkutils</groupId>
    <artifactId>quality_${qualityRuntime}${qualityShortVersion}_${scalaCompatVersion}</artifactId>
    <version>${qualityVersion}</version>
</dependency>
```

The qualityRuntime variable also supports further runtime types, such as Databricks.  

See the docs site https://sparkutils.github.io/quality/ for detailed instructions.
