## Why Quality?

When looking at the Data Quality options for a data mesh standard runtime offering we identified gaps in the available platforms, so we asked:
```
What would our Data Quality library look like? 
``` 
We ended up with a highly peformant and extensible row-level SQL based rule engine with low storage costs and a high degree of optimsation for both Spark and Databricks Runtimes.

## Gaps in existing Spark Offerings

[Deequ](https://github.com/awslabs/deequ) and [databricks dq](https://github.com/databrickslabs/dataframe-rules-engine) were unsuitable for the meshes requirements, crucially these tools (and others such as OwlDQ) could not run at low cost with tight SLAs, typically requiring processing the data once to get DQ and then once more to save with DQ information or to handle streamed data, not too surprising given their focus on quality across large data sets rather than at a row processing level as a first class citizen.  An important use case for DQ rules within this mesh platform is the ability to filter out bad rows but also to allow the consumer of the data to decide what they filter, requiring the producers results to ideally be stored with data rows themselves.  Additionally, and perhaps most importantly, they do not support arbitrary user driven rules without recoding.

As such our notional library needs to be:

* fast to integrate into existing Spark action without much overhead
* auditable, it should be clear which rule generated which results 
* capable of handling streamed data
* capable of being scripted
* integrate with DataFrames directly, also allowing consumer driven rules in addition to upstream producer DQ
* be able to fit results into a single field (e.g. a map structure of name to results) stored with the row at time of writing the results

## Resulting Solution Space

In order to execute efficiently with masses of data the calculation of data quality must scale with Spark, this requires either map functions, UDFs or better still Catalyst Expressions, enabling simple SQL to be used.  Storage of results for a row could be json, xml or using nested structures.

The evaluation of these solutions can be found in the [next](evaluation_method.md) sections.

## How did Rules and Folder come about?

Whilst developing a bookkeeping application a need for simple rules that generate an output was raised.  The initial approach taken, to effectively generate a case statement, ran into size and scale limitations.  The architect of the application asked - can you have an output sql statement for the DQ rules?  The result is QualityRules, although it should probably be called QualityCase...

QualityFolder came from a related application which had a need to transform data - providing defaulting in some circumstances - but still had to be  auditable and extensible as QualityRules was. 
