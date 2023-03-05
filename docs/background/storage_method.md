---
tags:
   - performance
---

# How should rule results be stored? -  JSON vs Structures

!!! note
    While Jackson is faster than circe serialization for JSON it doens't serialize easily so only used for comparison as its the fastest possible serialization framework.

## UDF Created Structures

When serializing rule results to Nested Rows via UDF struct creation (shown as Orange) the results are very expensive, the more complex the rule setup the worse the performance. In comparison Jackson (shown as blue) keeps a low cost as it's just a string (the cost instead is in parsing, storage and filtering)

![Performance of JSON vs UDF Structure Writing](../../img/serialisation_json_vs_udf_struct.png)

## Expression Created Structures

When serializing rule results with a custom Expression (shown as orange, using eval only - without custom compilation), Jackson (shown as blue) based serialisation looses it's clear lead with Expressions closing the gap as complexity increases:

![Performance of JSON vs Eval Expression Structure Writing](../../img/serialisation_json_vs_eval_expression_struct.png)

## Filtering Costs

Filtering on a nested column with deep queries (shown in red) is as expected faster the same query with a json structure.  Nested predicates can be pushed down to the underlying storage for efficient querying.

![Performance of JSON vs UDF Structure Writing](../../img/filtering_json_vs_struct.png)

!!! note
    Depending on the Databricks runtime used the benefit from seperating the overallResult field to a top level field can be 10-20% faster.  While each new release of Spark and DBR closes this gap it is recommended to use addOverallResultsAndDetailsF to split the fields.  
	This not only improves filter speed but also benefits with a simpler filter sql.
	
## Structure Model - storage costs

A naive structure representing RuleSuite, RuleSet and Rule results is actually less efficient than storage of JSON, however the current compressed model used by Quality has low overhead for even complex results.
