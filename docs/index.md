# Quality - {{project_version()}}

??? coverage "Coverage"
    
    <table>
    <tr>
        <td>Statement</td>
        <td class="coveragePercent">{{statement_coverage()}}:material-percent-outline:</td>
        <td>Branch</td>
        <td class="coveragePercent">{{branch_coverage()}}:material-percent-outline:</td>
    </tr>
    </table>

## Run complex data quality rules using simple SQL in a batch or streaming Spark application at scale.

Write rules using simple SQL or create re-usable functions via SQL Lambdas 

Your rules are just versioned data, store them wherever convenient, use them by simply defining a column.

* :new:{.pulseABit} - [rule_result](sqlfunctions/#rule_result) - directly access rule results from DQ and expressionRunner, simplifying row statistic collection 
* :new:{.pulseABit} - [expression runner](advanced/expressionRunner.md) - new runner type saving results directly, suitable for aggregate statistics
* :new:{.pulseABit} - [view loading](advanced/viewLoader.md) - load views for data lookup and transformation rules from a configuration DataFrame
* :new:{.pulseABit} - [map loading](advanced/mapFunctions.md#map-loading) - load maps using views or DataFrames from a configuration DataFrame
* :new:{.pulseABit} - [bloom loading](advanced/blooms.md#bloom-loading) - load blooms using views or DataFrames from a configuration DataFrame

Rules are evaluated lazily during Spark actions, such as writing a row, with results saved in a single predictable and extensible column.

## Enhanced Spark Functionality

Lookup Functions are distributed across the Spark cluster and held in memory, as such no shuffling is required where the shuffling introduced by joins may be too expensive:

* Support for massive [Bloom Filters](advanced/blooms/) while retaining FPP (i.e. several billion items at 0.001 would not fit into a normal 2gb byte array)
* [Map lookup](advanced/mapFunctions/) expressions for exact lookups and contains tests, using broadcast variables under the hood they are a great fit for small reference data sets


* [Lambda Functions](advanced/userFunctions/) - user provided re-usable sql functions over late binded columns


* Fast PRNG's exposing [RandomSource](https://commons.apache.org/proper/commons-rng/commons-rng-simple/apidocs/org/apache/commons/rng/simple/RandomSource.html) allowing plugable and stable generation across the cluster


* [Aggregate functions](advanced/aggregations/) over Maps expandable with simple SQL Lambdas


* [Row ID](advanced/rowIdFunctions/) expressions including guaranteed unique row IDs (based on MAC address guarantees)


Plus a collection of handy [functions](sqlfunctions.md) to integrate it all.
