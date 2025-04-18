### 0.1.3.1 <small>3rd December, 2024</small>

#78 - Allow extra plans to be added after a rewrite, ConstantFolding as a default given it gives a slight boost.

#76 - DBR 16.3 support - Databricks introduced a number of API changes not found in Spark 4, extra UnresolvedFunction params. (also includes #75)

#75 - DBR 15.4 support - Databricks introduced nonVolatile, a breaking change affecting all StatefulLike/Nondeterministic (rngs, uuids, unique_id), there is also a regression wrt interpreted Spark encoders (returning Stream and incorrect results) - the test cases have moved to Frameless encoders.

#68 - Test setup improvements for running testShades on Fabric (reduced logging and share Databricks behaviour)

#69 - Use different scopes for OSS testShade builds for Fabric testing (bug in snakeyml usage)

#70 - map_with can now be used in groupBy aggregations

#71 - Leverage Spark Sub-expression Elimination:

> In order to ensure behavioural compatibility this is not enabled on runners by default in 0.1.3.1.
> 
> To enable elimination ensure resolveWith = None (the default and not available in ExpressionRunner), compileEvals = false and forceRunnerEval = false (the default)
> 
> As part of this optimisation LambdaFunctions are rewritten and expanded as normal expression trees by a plan re-write. If this causes problems a `/* USED_AS_LAMBDA */` comment may be added to the LambdaFunction definition to disable this expansion for the entire sub-tree. 
> 
> The entire rewrite plan must be enabled by calling `com.sparkutils.quality.enableFunNRewrites()` within your SparkSession or by default via the Quality extensions.
> 
> NB The use of re-writes with 3.2.x has been identified in one test case (testSimpleProductionRules) as problematic for codegen, please use more recent Spark versions.

#73 - Spark 4.0 support (with an upgrade to Shim 0.2.0)

### 0.1.3 <small>4th October, 2024</small>

#53 - Docs parser is now more forgiving, empty descriptions are tolerated and normal scaladoc syntax is allowed

#50 - typedExpressionRunner - audited capture of expressions with the same type

#51 - Spark 3.5.0 support - NOTE ViewLoaderAnalysisException and MissingViewAnalysisException now have Exception causes

#27 - Delta 3.0.0 (Spark 3.5.0) support - compatible version

#55 - DBR 14.0/1 - Snake Yaml 2.0 support

#58 - Migrate custom runtime usage to Shim

#59 - DBR 13.3 LTS support

#57 - DBR 14.3 support

#61 - Use sparkutils frameless for 3.5, 13.3, 14.x builds - Due to encoding and shim changes this frameless fork version is not binary compatible with typelevel frameless proper 

#62 - SPARK-47509 workaround for Subqueries in lambdas - most common patterns are supported with 4.0 / 14.3 DBR

#63 - Use actual struct functions where possible for drop_field/update_field functions - required due to 14.3 DBR introduced plan on local relations

#66 - Bug fix - softFail result handling was double encoded - softFail result type is changed to double (breaking)

#65 - Bug fix - Incorrect OverallResult and string result processing  

### 0.1.2.1 <small>4th September, 2023</small>

Maven Central build issues, code wise the same as 0.1.2.

### [0.1.2](https://github.com/sparkutils/quality/milestone/7?closed=1) <small>4th September, 2023</small>

#48 - Bug fix - Enable Sub Queries in all runner types

### [0.1.1](https://github.com/sparkutils/quality/milestone/6?closed=1) <small>9th July, 2023</small>

#42 - Improve expression runner to store yaml, unlike json, to_yaml and from_yaml allow complete support for roundtripping of Spark data types

### [0.1.0](https://github.com/sparkutils/quality/milestone/3?closed=1) <small>10th June, 2023</small>

#29 - Quality OptimzerRule's run with Databricks sql display

#35 - agg_expr and associated lambda support in the functions package 

#36 - improved update_field, added drop_field based on the Spark withField (3.4.1 impl)

#34 - simplified quality package usage, column functions are now under the functions package.

#32 - expressionRunner - saves the results of expressions as strings, suitable for aggregate statistics

#28 - rule_result function - retrieves a rule result directly from a dq or expressionRunner result

#15 - Addition of the loadXConfigs and loadX functions for maps and blooms, simplifying configuration management

#24 - Remove saferId / rowid functions - use unique_id where required 

#18 - ViewLoader - simple view configuration via DataFrames  

#30 - 3.3.2 and 3.4.1 builds - simple version bumps

#20 - 3.5.0 starting support

### [0.0.3](https://github.com/sparkutils/quality/milestone/5?closed=1) <small>17th June, 2023</small>

#25 - Use builtIn function registration by default - allows global views to be created using Quality functions

### [0.0.2](https://github.com/sparkutils/quality/milestone/2?closed=1) <small>2nd June, 2023</small>

#16 - Remove winutils requirements for testing and usage

#13 - Support 3.4's sub query usage in rules/trigger, output expressions and lambdas 

#12 - Introduce the use of underscores instead of relying on camel case for function sql names, inline with Spark built-in functions

#10 - Base64 functions added for RowID encoding and decoding via base64 (more suitable for BI tools)

#9 - Add AsymmetricFilterExpressions with AsUUID and IDBase64 implementation, allows expressions used in field selects to be reversed, support added for optimiser rules through the SparkExtension 

#8 - Add set syntax for easier defaulting sql, removing duplicative cruft from intention

#7 - SparkSessionExtension to auto register Quality functions - does not work in 2.4, starting with this release 2.4 support is deprecated

#6 - Simple as_uuid function

#5 - Spark 3.4 and DBR 12.2 LTS support

#4 - comparableMaps / reverseComparableMaps functions, allowing map comparison / set operations (e.g. sort, distinct etc.)

### [0.0.1](https://github.com/sparkutils/quality/milestone/1?closed=1) <small>8th March, 2023</small>

Initial OSS version.

(many internal versions in between)

### the Quality exploration starts <small>25th April, 2020</small>

Start of investigations into how to manage DQ more effectively within Spark and the mesh platform.
