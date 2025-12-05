Starting with 0.2.0 and Spark 4 Quality leverages the new unified connect and classic APIs to enable connect friendly usage when used with the [SparkSessionExtension](../getting_started/#using-the-sql-functions-on-spark-thrift-hive-servers).  

This also opens the door for usage from any Spark Connect supporting language as the integration surface is much lower:

```scala
// versioned reads
def readVersionedRuleRowsFromDF(df: DataFrame, ruleSuiteId: Column,....): DataFrame
def readVersionedLambdaRowsFromDF(lambdaFunctionDF: DataFrame, lambdaFunctionName: Column,....): DataFrame
def readVersionedOutputExpressionRowsFromDF(outputExpressionDF: DataFrame, outputExpression: Column,....): DataFrame
// combine and register functions, which can use either simple or versioned reads 
def combine(ruleRows: Dataset[RuleRow], lambdaFunctionRows: Dataset[LambdaFunctionRow],
  outputExpressionRows: Dataset[OutputExpressionRow], probablePass: Double,
  globalLambdaSuites: Option[Dataset[Id]] = None, globalOutputExpressionSuites: Option[Dataset[Id]] = None): Dataset[CombinedRuleSuiteRows]
def register_rule_suite_variable(ds: Dataset[CombinedRuleSuiteRows], id: VersionedId, stableName: String): String
// version specific for lambdas
QualitySparkUtils.registerLambdaFunctions(functions: Seq[LambdaFunction])
```

with each function running on the server and connect using simple commands on views/tables, after any necessary renames etc.:

```sql
-- versioned reads
QUALITY VERSIONED RULES FROM DF viewName;
QUALITY VERSIONED LAMBDAS FROM DF viewName;
QUALITY VERSIONED OUTPUT EXPRESSIONS FROM DF viewName;
-- combine
QUALITY COMBINE RULESUITES ruleRowsName, lambdaFunctionRowsName | `None`,
  outputExpressionRowsName | `None`, probablePass Double | `None`,
  globalLambdaSuitesName | `None`, globalOutputExpressionSuitesName | `None`
QUALITY REGISTER RULE SUITE combinedRowsName, ruleSuiteId Int, ruleSuiteVersion Int, stableName
-- lambdas
CREATE QUALITY FUNCTION simplename _WITH_IMPL_ simpleExpression _END_OF_USER_FUNCTION_ 
    singleParamName _WITH_IMPL_ p1 -> simpleExpression _END_OF_USER_FUNCTION_
    multiParamsName _WITH_IMPL_ (p1, p2) -> simpleExpression _END_OF_USER_FUNCTION_
```

The loading and serialising functions register ruleSuites as Spark SQL Variables (via [DECLARE VARIABLE](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-declare-variable.html)/[SET VARIABLE](https://spark.apache.org/docs/latest/sql-ref-syntax-aux-set-var.html)) with all actual ruleSuite handling taking place on the server. 

The other non-loading functionality is represented as Spark Connect compatible sql function calls that require the [SparkSessionExtension](../getting_started/#using-the-sql-functions-on-spark-thrift-hive-servers).

This includes the runners themselves, which also get dsl equivalents:

* [dq_rule_runner](../../sqlfunctions/#dq_rule_runner)
* [rule_engine_runner](../../sqlfunctions/#rule_engine_runner)
* [rule_folder_runner](../../sqlfunctions/#rule_folder_runner)
* [typed_expression_runner](../../sqlfunctions/#typed_expression_runner)
* [expression_runner](../../sqlfunctions/#expression_runner)

as well as some new utility functions:

* [process_if_attribute_missing](../../sqlfunctions/#process_if_attribute_missing)

All SQL functions are simply forwarders to Spark Connects "call_function" implementation.

## Example Java Usage

_more to come, including json example_

```java
import static com.sparkutils.connect.functions.*;

public void functionUsingSpark(SparkSession session, DataFrame source) {
    DataFrame ruleRows = loadRules(session, columnNames /*...*/);
    DataFrame lambdaFunctionRows = loadLambdaFunctions(session, columnNames /*...*/);
    DataFrame outputExpressionRows = loadOutputExpressions(session, columnNames /*...*/);
    DataFrame combinedRows = combine(ruleRows, lambdaFunctionRows, outputExpressionRows, 0.8d);
    String name = register_rule_suite_variable(combinedRows, VersionedId(1,3), "ruleSuite");
    source.select(col("*"), dq_rule_runner(col(name)));
}
```

## What does CombinedRuleSuiteRows look like?

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

## What is not included in the connect support?

Essentially it's:

- blooms, these are memory intensive by default but may be targeted for later releases if demand is raised, 
- sparkless is, of course, distinctly Spark `Classic` in nature
- resolveWith
- validation, documentation
- enableFunRewrites (they are enabled, by default, on the extension side)