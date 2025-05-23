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

Write rules using simple SQL or create re-usable functions via SQL Lambdas.

Your rules are just versioned data, store them wherever convenient, use them by simply defining a column.

* :new:{.pulseABit} test packages for [Fabric](getting_started/running_on_fabric.md)
* :new:{.pulseABit} 5-10% speed bump with a change in compilation defaults
* :new:{.pulseABit} sparkless [row level processors](advanced/processors.md) added for non-spark runtimes (uses spark at compilation time)

Rules are evaluated lazily during Spark actions, such as writing a row, with results saved in a single predictable column.

## Enhanced Spark Functionality

Lookup Functions are distributed across the Spark cluster and held in memory, as such no shuffling is required where the shuffling introduced by joins may be too expensive:

* Support for massive [Bloom Filters](advanced/blooms/) while retaining FPP (i.e. several billion items at 0.001 would not fit into a normal 2gb byte array)
* [Map lookup](advanced/mapFunctions/) expressions for exact lookups and contains tests, using broadcast variables under the hood they are a great fit for small reference data sets
* [View loading](advanced/viewLoader.md) - manage the use of session views in your application through configuration and a pluggable [DataFrameLoader](./site/scaladocs/com/sparkutils/quality/DataFrameLoader.html)  

* [Lambda Functions](advanced/userFunctions/) - user provided re-usable sql functions over late bound columns


* Fast PRNG's exposing [RandomSource](https://commons.apache.org/proper/commons-rng/commons-rng-simple/apidocs/org/apache/commons/rng/simple/RandomSource.html) allowing pluggable and stable generation across the cluster


* [Aggregate functions](advanced/aggregations/) over Maps expandable with simple SQL Lambdas


* [Row ID](advanced/rowIdFunctions/) expressions including guaranteed unique row IDs (based on MAC address guarantees)


Plus a collection of handy [functions](sqlfunctions.md) to integrate it all.
