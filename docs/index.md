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

## Run complex data quality and transformation rules using simple SQL in a batch or streaming Spark application at scale.

Write rules using simple SQL or create re-usable functions via SQL Lambdas.

Your rules are just versioned data, store them wherever convenient, use them by simply defining a column.

* :new:{.pulseABit} Rule suite templates - derive and override rules to simplify repetitive configuration
* :new:{.pulseABit} Named projector - use rule suites to define projections
* :new:{.pulseABit} Lazy CSE - an experimental optimisation for large rule suites with a high number of common sub-expressions 

Rules are evaluated lazily during Spark actions, such as writing a row, with results saved in a single predictable column.

!!! warn "Databricks 18 changes it's release process"
    Per [this link](https://docs.databricks.com/aws/en/release-notes/runtime/18) Databricks will no longer have minor releases.
    Please pay attention to the [Running On Databricks](getting_started/running_on_databricks.md) page for exact versions tested against. 

## Enhanced Spark Functionality

* [Lambda Functions](advanced/userFunctions/) - user provided re-usable sql functions over late bound columns
* [Map lookup](advanced/mapFunctions/) expressions for exact lookups and contains tests, using broadcast variables on Classic and Variables on Connect under the hood they are a great fit for small reference data sets
* [View loading](advanced/viewLoader.md) - manage the use of session views in your application through configuration and a pluggable [DataFrameLoader](./site/scaladocs/com/sparkutils/quality/DataFrameLoader.html)  


* [Aggregate functions](advanced/aggregations/) over Maps expandable with simple SQL Lambdas
* [Row ID](advanced/rowIdFunctions/) expressions including guaranteed unique row IDs (based on MAC address guarantees)


* Fast PRNG's exposing [RandomSource](https://commons.apache.org/proper/commons-rng/commons-rng-simple/apidocs/org/apache/commons/rng/simple/RandomSource.html) allowing pluggable and stable generation across the cluster


Plus a collection of handy [functions](sqlfunctions.md) to integrate it all.
