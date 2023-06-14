### 0.1.0 <small>10th June, 2023</small>

#15 - Addition of the loadXConfigs and loadX functions for maps and blooms, simplifying configuration management

#24 - Remove saferId / rowid functions - use unique_id where required 

#17 - ViewLoader - simple view configuration via DataFrames  

#20 - 3.5.0 starting support

### 0.0.2 <small>2nd June, 2023</small>

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

### 0.0.1 <small>8th March, 2023</small>

Initial OSS version.

(many internal versions in between)

### the Quality exploration starts <small>25th April, 2020</small>

Start of investigations into how to manage DQ more effectively within Spark and the mesh platform.
