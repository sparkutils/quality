Quality prior to 0.2.0 can, despite various Spark and Quality optimisations, be fairly described as "brute forcing" rules processing.
This is, in part, due to three key Spark WholeStageCodeGen behaviours:

1. WholeStageCodeGen Sub Expression Elimination operates over an entire action stage (e.g. FilterExec or InputAdapter or HashJoinExec etc.)
2. Only "If" and "case" expressions provides any form of branching support
3. Sparks CodegenContext code generation approach more or less operates at an action stage

## Spark design choices limit complex evaluation performance

Simply put, Spark isn't designed to act as a rule engine, it just so happens to be a very, very, good general purpose row processor
and it's design is ideal for modeling Expressions, rows and having a clean optimisation approach. 

### Sub Expression Elimination is _slower_?

Per point 1. above, Sub Expression Elimination runs for all Expressions within a stage this, in the case of the BigRules test,
ends up with several 1000s of common sub expressions used throughout the 20k rules.  All of these sub expressions is evaluated
for each row, irrespective of whether or not they are even possible to be relevant for a row.  Picture, for example,

```sql
a = 'abcd' && b = 123 -- rule one
a = 'efgh' && b = 123 -- rule two
a = 'lmno' && b = 122 -- rule three
```

a will be compared against both 'abcd' and 'efgh' even if b is not 123 but is instead 122.  When testing with BigRules over
half of the cost for processing rules is spent evaluating useless combinations like this.

Unlike dedicated rule engines Spark has no graph of lazy evaluation that can pinpoint that "b" should be evaluated first.

### Branching is limited to If/case expressions

Given per 1. above we cannot delay evaluation of Sub Expressions, similarly evaluating the three above rules can only be
operated in sequence with Quality Folder and Collector as any matching trigger rule forces evaluation of their output expressions.

Ideally each 'top level' branch could be grouped and moved to a separate compilation unit wherein the sub expressions are actually useful:

```sql
-- group one
b = 123 -- then
    a = 'abcd' -- rule one
    a = 'efgh' -- rule two
-- group two  
a = 'lmno' && b = 122 -- rule three
```

Moreover, the 'a = string' pattern in group one could be generalised and compiled out to a switch statement (given a Janino version > 3.0.7). 

### CodegenContext is 'per stage'

At first this doesn't seem like a limitation, just a fact of life, but if the compilation units are split the input parameters, names and result handling all
needs to be managed.  Without cloning then rejoining the context the variable usage cannot remain consistent and various stages 
before compilation's current_vars are also difficult to manage.  All code within a stage is locked into a single compilation unit and
, unsurprisingly, although Janino is very fast it too struggles with 40mb+ java files to compile.  Split compilation is a must.

WholeStageCodeGen swaps a stack of common subexpressions to somewhat handle this but
variables often lose type information when not in sub expressions (or current_vars) and have compilation approaches to minimise class member
name pollution that hamper efforts to split compilation units.  

Differences in implementation between OSS and Databricks wrt. parameter handling and subexpressions also inhibit simple management of large code.

## Trigger Grouping as a solution

Quality 0.2.0 introduces separate compilation per "trigger group" via 'forking' CodegenContext and behaving similarly to a producer/consumer in wholestagecodegen.
This reduces compilation time drastically for the BigRules approach, taking it from minutes requiring 14gb down to 6/7 seconds requiring only 2gb.

Grouping also allows the subexpressions to be optimally located, sub expressions are evaluated at the point they are most needed and propagated to sub-group compilation units.

This moves the maximum number of top level evaluations for each row from the number of rules to the number of top level groups and their largest group depth.
RuleEngineRunner also applies an early exit approach:

- groups are ordered by salience, rules within groups similarly so
- if a rule passes within a group of triggers it can early exit
- if a rule has passed within a group of groups, the salience of the following groups is evaluated for possible early exit

These approaches, with the TopLevelBooleanGrouper, led to the performance for the 20k "BigRule" per row test case to increase from 3ms per row down to sub 0.03ms per row.

### Config Options

Quality now allows customisable grouping of Triggers via a newly optional extraConfig parameter:

```scala
extraConfig = Map(
  groupProcessorKey /* "quality.runnerGroupProcessor" */ -> classOf[TopLevelBooleanGrouper].getName
))
```

These parameters can be provided directly when calling a specific runner or via System properties and Spark conf
which apply across all runners.  It is possible to provide a custom grouping approach by extending the catchily named GroupBasedGrouper,
compilation handling will be managed but the grouping itself needs to be provided.  

The experimental TopLevelBooleanGrouper, in addition to whole stage compilation improvements and #131, allows reduction of evaluation
cost by 20x in the test case, grouping by common "and" expressions and bucketing via " field = 'value' " comparisons.

These buckets can be configured by the:

```scala
groupProcessorBucketSizeKey /* "quality.runnerGroupProcessor.bucketSize" */: Int = 130, 
groupProcessorPercentFilter /* "quality.runnerGroupProcessor.percentFilter" */: Double = 0.01
```

parameters, which aim to manage a target bucket size of 130, and is used as a guide in the bucketing approach.
The filter removes expressions from groups that appear in less than only present in 0.01% of the overall expressions.

An optional extraConfig parameter of:

```scala
groupProcessorDumpAuditKey /* "quality.runnerGroupProcessor.dumpAudit" */ : Boolean = false,
groupProcessorAuditLocation /* "quality.runnerGroupProcessor.auditLocation" */ : String = "./" 
```

will generate a RuleSuiteGroup file using the specified cross cluster location (this is required on a Databricks for example).

This group contains a RuleSuite(0,0) 'parent' Rule Suite that calls the child 'bucket' rule suites,
using the same grouping as the normal TopLevelBooleanGrouper uses, but in an auditable and executable form for easy
verification of bucketing correctness.

### TopLevelBoolean grouping

This initial experimental version of the general TopLevelBoolean grouper brings the BigRules test case 
runtime from 2m on an AMD Ryzen AI 9 HX 370 (requiring -Xmx16g) to under
24s (requiring only -Xmx2g) and a per row Quality processing time of sub 0.03ms per row (down from 3.68 #137 or >5ms pre #137) on a 20k rule RuleSuite
(across max 9 comparisons per rule, 380m expressions in total).
If the results from a non-grouped default runner configuration differ from a grouping approach please raise an issue (Unresolved is however expected on RuleEngineRunner).

TopLevelBooleanGrouper cannot work on 3.0 or 3.1 and, although functional on 3.2 / 3.21, is only recommended on 3.3 and
above as 3.2's performance is slower overall due in part to still requiring sub expressions to be evaluated multiple extra times for the entire tree.
3.3 and above only uses subexpressions within the runner itself as needed by the groups.

Per the grouping example above:

```sql
-- group one
b = 123 -- then
    a = 'abcd' -- rule one
    a = 'efgh' -- rule two
-- group two  
a = 'lmno' && b = 122 -- rule three
```

The runner has a separate compilation unit (typically the Spark stage has only a call to generate then a per row call to apply), which in turn
has two groups below, each in their own compilation unit.

NB: Testing on a cluster (e.g. Databricks or Fabric) will require 64gb, the expression trees are too large to deserialize
on the executors with less RAM and is not a limitation of these platforms, just an artefact of Spark.

### SwitchSupport

Groups which have trigger rules that, after grouping, all match the pattern 'field = literal', where the literal is a string or integral type,
can be converted to Java switches.  The SwitchSupport runs independently of actual GroupBasedGrouper implementation and
applies a binary branching strategy across multiple smaller functions each with a switch statement to allow inlining as appropriate.

If it is not possible to use a switch the behaviour degrades back to whatever the groupProcessorKey's approach is,
TopLevelBoolean for example will use mod's across any field against literal groupings.
