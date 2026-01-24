---
tags: advanced
---
   
collectRunner is expressly designed for the following rule pattern:

| Rule ID | Salience | Rule                                  | Output Expression                         |
|---------|----------|---------------------------------------|-------------------------------------------|
| 1       | 100      | a = '5' and b is not null and c > 100 | array(outputrow1, outputrow2, outputrow3) |
| 2       | 200      | a = '5' and c > 100                   | array(outputrow4)                         |

Where the row (a,b,c) (5, 'value', 150) should have the output:

| Result                                                |
|-------------------------------------------------------|
| array(outputrow1, outputrow2, outputrow3, outputrow4) |

i.e. the Output Expressions of all matching rules should be added and flattened in salience order. 

Quality prior to 0.1.4 offered ruleFolder as a general case 'run all the things which match' engine.  The above pattern can be represented by:

| Rule ID | Salience | Rule                                  | Output Expression                                                                               |
|---------|----------|---------------------------------------|-------------------------------------------------------------------------------------------------|
| 1       | 100      | a = '5' and b is not null and c > 100 | set(resultArray = concat(currentResult.resultArray, array(outputrow1, outputrow2, outputrow3))) |
| 2       | 200      | a = '5' and c > 100                   | set(resultArray = concat(currentResult.resultArray, array(outputrow4)))                         |

This is functionally identical but each rule involves two additional array creations and array copy's.  
If this wasn't expensive enough the use of a Spark LambdaFunction disables all subexpression eliminations within those Output Expressions.

The CollectorThroughputBenchmark shows the following indicative results against 1m rows using 50 rules (more than this is not possible to compile with the pure Spark approach):

| Run against                                                                     | Mean time taken in ms |
|---------------------------------------------------------------------------------|-----------------------|
| Spark SQL flatten(filter(array(if(rule, line1, null), if(rule2, line2, null)).. | 1316.74               |
| Quality Folder                                                                  | 912.70                |
| Quality Collector                                                               | 630.29                |

collectRunner fixes this by efficient array allocations at only one per row, and by default auto flattening nested calls to array.

!!! info "How is it faster than normal Spark SQL?"
    collectRunner swaps out calls to the "array" function and instead passes on the result of each array member directly to a single 
    temporary array per row without creating the interim array or therefore requiring an array copy.

    This in turn can be optionally filtered out for nulls without requiring intermediatary instances.

    So the equivalent spark of:

    ```sql
    filter(array(if(a, null, oa), if(b, null, ob)), x -> x IS NOT NULL)
    ```

    which is the same as using array_compact, requires at least three array creations as well as the overhead of the lambda, 
    which as per folderRunner cannot take part in sub expression elimination and other optimisation strategies.  
    Indeed filter currently cannot have subexpression elimination applied at all, this also includes the array input.   
    Of course that also comes with the lack of an audit trail.