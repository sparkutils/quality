---
tags:
   - performance
---

# How should rules be evaluated?

Performance wise there is a clear winner as to approach for generating results:

![Performance of Map vs WithColumn](../../img/mapvsextrafield.png)

The green row is using the map function which is unfortunately the most straightforward to program.  The blue is the baseline of processing a row without DQ and the orange is using withColumn.

withColumn can use UDFs or inbuilt Catalyst style functions - the latter giving better performance and ability to more naturally integrate with spark, [this review echos the findings](https://medium.com/@fqaiser94/udfs-vs-map-vs-custom-spark-native-functions-91ab2c154b44) and hinting at the effects of catalyst.

**Overall storage winner is nested columns**, it has lower storage costs, is as fast as json to serialize (via an Expression) and faster to query with predicate push down support for faster filtering.
Details of the analysis are below.

!!! note
    Using withColumn is strongly discouraged, it very quickly introduces performance issues in spark code, prefer to use select and the Quality transform functions.
    A large part of the performance hit for using UDFs over Expressions is due to the conversion from user types to InternalRow - this cannot be avoided.
   
## What does the performance actually look like?

Run on Spark 4.1 [the following](https://sparkutils.github.io/quality_performance_tests/reports/report_rc7preview3_4.1_pure_spark/) shows
a simple DQ run with 15 rules, with a baseline ["boolean array"](https://github.com/sparkutils/quality_performance_tests/blob/main/src/main/scala/com/sparkutils/quality_performance_tests/PerfTests.scala#L90) result (the bottom orange) and an audit trail version using [pure spark](https://github.com/sparkutils/quality_performance_tests/blob/main/src/main/scala/com/sparkutils/quality_performance_tests/PerfTests.scala#L112) (the top blue line):   

![Performance of Quality vs Spark SQL](../../img/0.2.0_rc7_dq_perf.png)

The middle green line is the performance of Quality (at about 0.014ms per row's 15 rules) with user functions [abstracting repetitive](https://github.com/sparkutils/quality_performance_tests/blob/main/src/main/scala/com/sparkutils/quality_performance_tests/PerfTests.scala#L41) case statement logic.
Of note is that although the Quality rule processing is faster this is only true in Spark 4.1 if the FunNRewrite optimisation is used, e.g. via enableFunNRewrites.

## How big can the rule suite be?

With Quality 0.2.0 rule engine and TopLevelBooleanGrouper runs of exact boolean 'and' matches (e.g. truth tables) can process a 20k RuleSuite in sub 0.03ms per row, or 3.68ms per row when not using grouping.

Please reach out if there are interesting performance bottlenecks or even larger RuleSuite sizes and complexity.  
