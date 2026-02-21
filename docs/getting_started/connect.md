Starting with 0.2.0 and Spark 4 Quality leverages the new unified connect and classic APIs to enable connect friendly usage when used with the [SparkSessionExtension](../getting_started/#using-the-sql-functions-on-spark-thrift-hive-servers).

This allows applications to use the quality_api with the stable Spark Connect interface and upgrade their server implementation of Quality without impacting others on the Shared Cluster (other than a restart of course). 
In order to enable this, from 0.2.0 onwards, Quality's moves to a split jar and implementation model:

* Core Scala types are present in quality_core, each - now source stable for years - are marked with @SerialVersionUID(1L)
* quality_api provides the Connect friendly api, using these core types, with most complex logic taking place on the server
* The quality server jar itself provides the implementation found in the Quality extension and serializes the core jvm types

Breaking binary compatibility of the core types is now identifiable via their @SerialVersionUID(1L) changes.  Changes to the core types will include increasing the version id.

Given the stable Connect interface, and a suitable runtime, this _should_ allow for client applications to build against versions of Spark Connect for their Scala version only but enjoy running on multiple backends.

Supported combinations:

| Spark Runtime                 | Known Supported Version | Client application Library | 
|-------------------------------|-------------------------|----------------------------| 
| Databricks Shared Compute     | 17.3                    | quality_api_17.3           |
| Databricks Shared Compute     | 17.3                    | quality_api_4.0.0.oss      |
| Databricks Non Shared Compute | 17.3                    | quality_api_17.3           |
| Databricks Non Shared Compute | 17.3                    | quality_api_4.0.0.oss      |
| OSS Spark connect.local       | 4.0.*                   | quality_api_17.3           |

This is also true for remote connect usage, for example running the QualityTestRunner in connect mode from the ide to Databricks with the following environment variables:

```scala
SPARKUTILS_TEST_OUTPUTDIR abfss://XXX
SPARK_REMOTE sc://adb-XXX.azuredatabricks.net:443/;token=dapiXXX;x-databricks-cluster-id=XXX
SPARKUTILS_DISABLE_CLASSIC_TESTS true
SPARKUTILS_CONNECT_CLIENT.fs.azure.account.key.XXX.dfs.core.windows.net XXXXXXX
```

works without registering jars via Connect client (unless UDF / maps are used) or on the cluster itself (aside from extension).  
Assuming Databricks 18.x uses 4.1 and the connect code is compatible it _should_ be possible to use quality_api_4.0.0.oss against it as well.

This client/server split allows shared servers to upgrade their Quality extension library without forcing each client to upgrade their client version unless the protocol changes (either Spark Connect or quality_api/core).  

## What is not included in the connect support?

Essentially:

- blooms, these are memory intensive by default but may be targeted for later releases if demand is raised,
- sparkless is distinctly Spark `Classic` in nature
- resolveWith
- validation, documentation
- enableFunRewrites (they are enabled, by default, on the extension side)

Similarly, Databricks serverless is not possible as there is no SparkSessionExtension support for serverless (this is also, of course, true for Classic Quality). 

## How to build applications against Connect with an Extension?

In order to build your own connect client and server extension jars you should follow:

* the example approach found in the [test shade pom](https://github.com/sparkutils/quality/blob/temp/0.2.0/testShades/pom.xml#L517) to select resources and
* the [scripting jarjar approach](https://github.com/sparkutils/quality/blob/temp/0.2.0/testShades/pom.xml#L570) and [shade rules](https://github.com/sparkutils/quality/blob/main/shade.rules) if you wish to re-use the jar for encoding with implicits

The key difference is which jar you build against (the testShades pom illustrates this via the client and server [profiles](https://github.com/sparkutils/quality/blob/temp/0.2.0/testShades/pom.xml#L31)).

* To build your own connect jar, depend on the appropriate quality_api jar only (OSS 4.0.0 and onwards should be sufficient), or
* To build your own server extension jar, depend only on the full quality runtime jar (this will already exclude the api_stub jar)

NB Using the appropriate runtime quality_testshade jar may likely be enough for the server side extension.  

??? info "Why are there duplicate classes warnings from client shade?"
    When building a shade you may see "overlapping classes" warnings (this example is for quality_connect_testshade): 
    
    ```
    [WARNING] quality_core_4.0.0.oss_4.0_2.13-0.2.0-SNAPSHOT.jar, quality_api_4.0.0.oss_4.0_2.13-0.2.0-SNAPSHOT.jar, quality_api_stub_4.0.0.oss_4.0_2.13-0.2.0-SNAPSHOT.jar define 2 overlapping classes:
    [WARNING]   - com.sparkutils.shim.EmptyCompilationHelper
    [WARNING]   - com.sparkutils.shim.EmptyCompilationHelper$
    [WARNING] quality_api_4.0.0.oss_4.0_2.13-0.2.0-SNAPSHOT.jar, quality_api_stub_4.0.0.oss_4.0_2.13-0.2.0-SNAPSHOT.jar define 2 overlapping classes:
    [WARNING]   - com.sparkutils.quality.impl.mapLookup.MapLookup$
    [WARNING]   - com.sparkutils.quality.impl.mapLookup.MapLookup
    [WARNING] maven-shade-plugin has detected that some class files are
    [WARNING] present in two or more JARs. When this happens, only one
    [WARNING] single version of the class is copied to the uber jar.
    [WARNING] Usually this is not harmful and you can skip these warnings,
    [WARNING] otherwise try to manually exclude artifacts based on
    [WARNING] mvn dependency:tree -Ddetail=true and the above output.
    [WARNING] See http://maven.apache.org/plugins/maven-shade-plugin/
    ```
    
    These can be ignored.  The "EmptyCompilationHelper" from shim (and other sparkutils jars) is simply an empty class to force package object scala docs to be built.
    
    The quality MapLookup warnings deserve further explanation, in order to re-use the implementation but provide a consistent interface on both connect and classic
    quality_api_stub, as the name suggests, provides stub implementations that are then swapped out by quality_api and the classic quality 'server' jar as appropriate.  

??? warn "Keep Connect and Server code separated"
    If your code uses both Classic functions and Connect functions in the same object you may force verify or implementation changed errors as this code is not present in normal Spark connect client runtimes.  

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

## Integration Options

Spark Connect, combined with the Quality extension, opens the door for usage from any Spark Connect supporting language as the integration surface is much lower:

```scala
// versioned reads
def readVersionedRuleRowsFromDF(df: DataFrame, ruleSuiteId: Column,....): DataFrame
def readVersionedLambdaRowsFromDF(lambdaFunctionDF: DataFrame, lambdaFunctionName: Column,....): DataFrame
def readVersionedOutputExpressionRowsFromDF(outputExpressionDF: DataFrame, outputExpression: Column,....): DataFrame
def readVersionedRuleSuitesFromDF(ruleSuitesDF: DataFrame, ruleSuiteId, rulesSuiteVersion,....): DataFrame
// combine and register functions, which can use either simple or versioned reads 
def combine(ruleRows: Dataset[RuleRow], lambdaFunctionRows: Dataset[LambdaFunctionRow],
  outputExpressionRows: Dataset[OutputExpressionRow], probablePass: Double,
  globalLambdaSuites: Option[Dataset[Id]] = None, globalOutputExpressionSuites: Option[Dataset[Id]] = None,
  ruleSuites: Option[Dataset[RuleSuiteRow]] = None): Dataset[CombinedRuleSuiteRows]
def register_rule_suite_variable(ds: Dataset[CombinedRuleSuiteRows], id: VersionedId, stableName: String): String
// version specific for lambdas
QualitySparkUtils.registerLambdaFunctions(functions: Seq[LambdaFunction])
```

with each function running on the server and connect using simple commands on local non-global temp views, after any necessary renames etc.:

```sql
-- versioned reads
QUALITY VERSIONED RULES FROM DF viewName; -- With columns: ruleSuiteId, ruleSuiteVersion, ruleSetId, ruleSetVersion, ruleVersion, ruleExpr, ruleEngineSalience, ruleEngineId, ruleEngineVersion
QUALITY VERSIONED LAMBDAS FROM DF viewName; -- With columns: name, ruleExpr, functionId, functionVersion, ruleSuiteId, ruleSuiteVersion
QUALITY VERSIONED OUTPUT EXPRESSIONS FROM DF viewName; -- With columns: ruleExpr, functionId, functionVersion, ruleSuiteId, ruleSuiteVersion
QUALITY VERSIONED RULESUITES FROM DF viewName; -- With columns: functionId, functionVersion, probablePass, ruleSuiteId, ruleSuiteVersion
-- combine
QUALITY COMBINE RULESUITES ruleRowsName, lambdaFunctionRowsName | `None`,
  outputExpressionRowsName | `None`, globalLambdaSuitesName | `None`, 
  globalOutputExpressionSuitesName | `None`, ruleSuitesName | `None`
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

### What does CombinedRuleSuiteRows look like?

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
 probablePass DOUBLE,
 defaultProcessor STRUCT<
   ruleExpr: STRING, functionId: INT NOT NULL, functionVersion: INT NOT NULL, 
   ruleSuiteId: INT NOT NULL, ruleSuiteVersion: INT NOT NULL
 >
```

