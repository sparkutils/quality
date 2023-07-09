---
tags:
- basic
- getting started
- beginner
---

Quality has four main flavours with sprinklings of other Quality ingredients like the [sql function suite](../sqlfunctions.md).

These flavours are provided by four "runners" which add a Column to a Spark Dataset/Dataframe.

## Quality / QualityData - ruleRunner 

Execute SQL based [data validation](a_first_rulesuite.md) rules, capture all the results and store them *with* your data for easy and fast access.

_Example Usage:_ Validating in-bound data or the results of a calculation.

_What is stored:_ 
```plantuml format="svg_object" classes="shrink_to_fit" source="docs/getting_started/dq.puml" 
```

## QualityRules - ruleEngineRunner

[QualityRules](../advanced/ruleEngine.md) extends the base Quality framework to provide the ability to generate output based on a single SQL rule matching the input data. Effectively an auditable large scale SQL case statement.

Conceptually trigger rules are the _when_ and Output rules are the _then_ ordered by salience.

_Example Usage:_ Derivation Logic.

_What is stored:_ 
```plantuml format="svg_object" classes="shrink_to_fit" source="docs/getting_started/rules.puml"
```

## QualityFolder - ruleFolderRunner

[QualityFolder](../advanced/ruleFolder.md) extends QualityRules providing the ability to change values of attributes based on any number of SQL rules matching the input data.

Unlike QualityRules which uses salience to select only one Output expression, Folder uses salience to order the execution of *all* the matching Trigger's paired Output Expressions - [folding](https://en.wikipedia.org/wiki/Fold_(higher-order_function)#:~:text=In%20functional%20programming%2C%20fold%20(also,constituent%20parts%2C%20building%20up%20a)) the results as it goes. 

_Example Usage:_ Correction of in-bound data to enable subsequent calculators to process, defaulting etc.

_What is stored:_ 
```plantuml format="svg_object" classes="shrink_to_fit" source="./docs/getting_started/folder.puml"
```

## QualityExpressions - ExpressionRunner

[QualityExpressions](../advanced/expressionRunner.md) extends QualityRules providing the raw results as yaml strings (with type) for expressions and allowing aggregate expressions.

_Example Usage:_ Providing totals or other relevant aggregations over datasets or DQ results - e.g. only deem the data load correct when 90% of the rows have good DQ.

_What is stored:_
```plantuml format="svg_object" classes="shrink_to_fit" source="./docs/getting_started/expressionRunner.puml"
```
