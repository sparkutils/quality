### [0.2.0](https://github.com/sparkutils/quality/milestone/10?closed=1) <small>24th December, 2025</small>

This release migrates Spark 4 support to use AgnosticEncoders and removes EOL runtimes: 2.4 and DBR's 9.1, 10.4, 11.3, 13.1 and 14.0.  
Spark runtimes 3, 3.1.3, 3.2.0, 3.2.1 and 3.3.2 are deprecated as are DBR's 12.2 and 13.3 and will be removed as of Quality version 0.3.0. 

#90 - Migrate to Spark 4 sql-api, AgnosticEncoder's and support Connect:

> The Sparkutils libraries Shim 0.3.0, Testing 0.1.0 and Frameless 2.0.0 provide support for
> custom encoding via AgnosticEncoders and a stable API that works with both Spark Classic and Connect.
> 
> To use Connect the Quality SparkSessionExtension must be enabled on the "server" Spark Driver side, 
> the connect friendly DSL forwards all the complexity to the Connect Server.  The newly released Testing project
> is used to run the same test cases against both Classic and Connect ensuring the API is stable.  This also extends 
> to user functions (LambdaFunctions), on Connect they are sent to the server via a custom command.
> 
> The Testless cluster notebook testing experience has been abstracted to the Testing project providing a standard  
> interface Quality Scalatest's on clusters, now shared with all Sparkutils testshade based projects.  On Databricks
> the testing runs will also run in the normal Classic mode and, via the scala.api.mode=connect config parameter, 
> against the provided Spark Connect server.
> 
> A number of functions are not possible to run in Connect and are provided with _classic as a suffix, these typically
> relate to extension points such as monadic add.
> 
> Map and Bloom related functions from 0.2.0 Spark 4 onwards allow multiple lookups to be used and leverage a 
> struct [Spark Variable](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-declare-variable.html#:~:text=Temporary%20variables%20are%20scoped%20at,a%20column%20or%20column%20alias.).
> This change swaps the last parameter type of the DSL, and introduces a third breaking change parameter for the SQL interface, to refer to the Spark Variable, with each map being a strongly typed member of the variable available for use with any Spark queries (although probably not all too useful for blooms).
> 
> If code was using QualitySparkUtils the import is now ClassicQualitySparkUtils.

#96 - quality_api is introduced, leveraging Spark Connect - allows a client server model and further client language support

#100 - Support for key functions to be run from the SparkSessionExtension when using quality_api, reducing the integration surface area for other client languages and simplifying upgrades

#108 - Support for Spark 4.1 added

#72 - Defaults for sub expression elimination and compilation of triggers are changed to better overall performance with recent Spark versions. 

> compileEvals and forceTriggerEval now default to false for all runner types and are removed for the connect api, as are resolveWith.  This has been found to be the best balance for most rules with 
> large performance gains as of 0.1.3.1 for long-running processes or larger data volumes.  These can be set to the previous defaults for the old behaviour if
> code generation itself dominates your applications time but note that nesting and chaining calls between runners is not supported - use .cache / write interim results if this is needed.
> ruleEngineRunner's schema parameter when using all parameters is now Option\[DataType\], wrap in Some if you are using custom parameters for false etc.not relying on the old defaults.
> Deriving the type for ruleEngineRunner may work but you must use the type if control over nullability is required (for example expressions differ in nullability).

#87 - EOL DBR and Spark runtimes are removed: 9.1, 10.4, 11.3, 13.1, 14.0

#21 - Remove 2.4 Support, Tech Debt removal

#112 - Change overallResult to correctly reflect the ruleEngine processing (if any Passed is present then the overall should be Passed)

### [0.1.4](https://github.com/sparkutils/quality/milestone/10?closed=1) <small>24th February, 2026</small>

This release provides a new runner type - collectRunner and a new RuleResult type of ignoredRule.

Similar to folder, in that it runs Output Expressions for each
matching trigger Rule expression ordered by salience, it collects the result of each Output Expression.  
It is optimised around optionally collecting and expanding nested arrays and auto expanding User LambdaFunctions.

If no matching trigger rule is found the new optional RuleSuite.DefaultProcessor can be run and the overallResult will reflect DefaultRule.  
In 0.2.0 this default approach will also be extended to engine and folder runners (with an optional fallback configuration for the 0.2.x series).

0.1.4 was released in order to speed up delivery of 0.2.0 functionality for some key users.  Please note, at time of publishing, many of the published Databricks and OSS versions are un-supported by their communities, please consider migrating versions to 14.3 LTS and Spark 3.5 at a minimum.

#107 - Introducing collectRunner - an optimised collecting rule engine

#111 - Introducing the notRelevant result - signal that a rule was not relevant for a row but is equivalent to a pass.  Allows collecting statistics in three states, rows passed, rows failed and rows not relevant for a given rule.

#93 - Introducing the rule_suite_statistics aggregate function, which collects statistics over a RuleSuiteResult column

#114 - Further improvements to FunNRewrite logic, allowing both more control over the optimisation and more use cases where it can optimise.

### [0.1.3.1](https://github.com/sparkutils/quality/milestone/10?closed=1) <small>24th October, 2025</small>

This is the last release of 2.4, 3.0 is deprecated as of this release, similarly, Databricks versions 9.1 through to 11.3 are also now deprecated and unsupported functionality (fixes for #84 will be gladly accepted), 12.2 support will continue.

#95 - DBR 17.3 Support - Databricks introduced a binary change to NamedExpression 

#85 - Processor optimisation to provide a passed result

#83 - Processor optimisations for stateless and Higher Order Functions

#82 - Wholestage codegen support for Correlated Subqueries, improved support for pass-through fields from plans

#81 - Enable Quality row level runners to be used outside a spark runtime (still requires spark to build of course)

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

#73 - Spark 4.0 support (with an upgrade to Shim 0.2.0 using sparkutils.frameless 1.0.0)

### [0.1.3](https://github.com/sparkutils/quality/milestone/8?closed=1) <small>4th October, 2024</small>

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
