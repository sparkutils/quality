Starting with 0.2.0 and Spark 4 Quality leverages the new unified connect and classic APIs to enable connect friendly usage.  

This also opens the door for simpler Python and Java usage as the integration surface is much lower:

```scala
  def combine(ruleRows: Dataset[RuleRow], lambdaFunctionRows: Dataset[LambdaFunctionRow],
              outputExpressionRows: Dataset[OutputExpressionRow], probablePass: Double): Dataset[CombinedRuleSuiteRows]
  def register_rule_suite_variable(ds: Dataset[CombinedRuleSuiteRows], id: VersionedId, stableName: String): String
```

Users load CombinedRuleSuiteRows (or the equivalent DataFrame) representing the following DDL:

```sql
ruleSuiteId INT NOT NULL,ruleSuiteVersion INT NOT NULL,
 ruleRows ARRAY<
    STRUCT<ruleRow: STRUCT<
            ruleSuiteId: INT NOT NULL, ruleSuiteVersion: INT NOT NULL, ruleSetId: INT NOT NULL, 
            ruleSetVersion: INT NOT NULL, ruleId: INT NOT NULL, ruleVersion: INT NOT NULL, 
            ruleExpr: STRING, ruleEngineSalience: INT NOT NULL, ruleEngineId: INT NOT NULL, 
            ruleEngineVersion: INT NOT NULL
        >, 
        outputExpressionRow: STRUCT<
            ruleExpr: STRING, functionId: INT NOT NULL, functionVersion: INT NOT NULL, 
            ruleSuiteId: INT NOT NULL, ruleSuiteVersion: INT NOT NULL
        >
    >
 >,
 lambdaFunctions ARRAY<
    STRUCT<
        name: STRING, ruleExpr: STRING, functionId: INT NOT NULL, functionVersion: INT NOT NULL, 
        ruleSuiteId: INT NOT NULL, ruleSuiteVersion: INT NOT NULL
    >
 >,
 probablePass DOUBLE
```

