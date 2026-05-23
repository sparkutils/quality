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

* :new:{.pulseABit} Spark 4.1 and 4.x [Connect Support](getting_started/connect/)
* :new:{.pulseABit} Folder can use a DefaultProcessor, both Folder and Engine now use the improved collectRunner result processing logic
* :new:{.pulseABit} RuleSuiteGroups, manage a single group of rules by name and use it to access ruleSuites in nested runners and group the results
* :new:{.pulseABit} Improved compilation performance for large scale RuleSuites by separate compilation
* :new:{.pulseABit} Experimental and optional support for optimised large scale rules (>20k RuleSuites) with 2x speed improvements and lower memory requirements via TriggerGrouper

Rules are evaluated lazily during Spark actions, such as writing a row, with results saved in a single predictable column.

## Enhanced Spark Functionality

* [Lambda Functions](advanced/userFunctions/) - user provided re-usable sql functions over late bound columns
* [Map lookup](advanced/mapFunctions/) expressions for exact lookups and contains tests, using broadcast variables on Classic and Variables on Connect under the hood they are a great fit for small reference data sets
* [View loading](advanced/viewLoader.md) - manage the use of session views in your application through configuration and a pluggable [DataFrameLoader](./site/scaladocs/com/sparkutils/quality/DataFrameLoader.html)  


* [Aggregate functions](advanced/aggregations/) over Maps expandable with simple SQL Lambdas
* [Row ID](advanced/rowIdFunctions/) expressions including guaranteed unique row IDs (based on MAC address guarantees)


* Fast PRNG's exposing [RandomSource](https://commons.apache.org/proper/commons-rng/commons-rng-simple/apidocs/org/apache/commons/rng/simple/RandomSource.html) allowing pluggable and stable generation across the cluster


Plus a collection of handy [functions](sqlfunctions.md) to integrate it all.
