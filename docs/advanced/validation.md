Quality provides some validation utilities that can be used as part of your rule design activity to ensure sure you aren't using variables or functions that don't exist, or even possibly having recursive lambda calls.

It comes in two distinct flavours:

1. Schema Based - The schema representing your dictionary
2. DataFrame Based - Use an actual DataFrame to provide your dictionary

with the option of running the rules against your schema (or DataFrame) via the runnerFunction parameter.

A simpler function for just assessing known Errors against a schema are also provided:

```scala
def validate(schema: StructType, ruleSuite: RuleSuite): Set[RuleError]
``` 

The validation result model is as follows:

```plantuml title="Validation Results Model"
Id *-- HasId
HasId <-- RuleError
HasOutputText <-- RuleError
HasId <-- RuleWarning
HasOutputText <-- RuleWarning

RuleError <-- SyntaxError
RuleError <-- NameMissingError

RuleWarning <-- LambdaPossibleSOE
LambdaPossibleSOE ---> LambdaRelevant
RuleWarning <-- SyntaxWarning 
SyntaxWarning <-- SyntaxNameWarning
SyntaxNameWarning <-- ExtraDocParameter
ExtraDocParameter ---> LambdaRelevant
SyntaxWarning <-- NonLambdaDocParameters

SyntaxError <-- LambdaSyntaxError
LambdaSyntaxError ---> LambdaRelevant 
SyntaxError <-- LambdaStackOverflowError
LambdaStackOverflowError ---> LambdaRelevant 
SyntaxError <-- RuleSyntaxError
RuleRelevant <--- RuleSyntaxError
SyntaxError <-- OutputRuleSyntaxError
OutputExpressionRelevant <--- OutputRuleSyntaxError
SyntaxError <-- DataFrameSyntaxError
SyntaxError <-- LambdaMultipleImplementationWithSameArityError
LambdaMultipleImplementationWithSameArityError ---> LambdaRelevant

NameMissingError <-- LambdaNameError
LambdaNameError ---> LambdaRelevant
NameMissingError <-- RuleNameError
RuleRelevant <--- RuleNameError
NameMissingError <-- OutputRuleNameError
OutputExpressionRelevant <--- OutputRuleNameError
NameMissingError <-- LambdaSparkFunctionNameError
LambdaSparkFunctionNameError ---> LambdaRelevant
NameMissingError <-- SparkFunctionNameError
RuleRelevant <--- SparkFunctionNameError
OutputExpressionRelevant <--- OuputSparkFunctionNameError 
NameMissingError <-- OuputSparkFunctionNameError

LambdaViewError ---> LambdaRelevant
RuleViewError ---> RuleRelevant
OutputRuleViewError ---> OutputRuleRelevant
ViewMissingError <-- RuleViewError
ViewMissingError <-- OuputRuleViewError
ViewMissingError <-- LambdaViewError


class Id {
  Int id
  Int version
}

class HasId {
  Id id
}

class RuleError {
  String data
  Boolean syntax = False
  String error
  String errorText
}

class RuleWarning {
  String warning
  Boolean syntax = False
  String warningText
}

class SyntaxWarning {
  Boolean syntax = true
}

class SyntaxNameWarning {
  def name: String
}

class SyntaxError {
  Boolean syntax = True
}

class NameMissingError {
}
```

so the simple version returns any known Errors backed by case classes so you can pattern match as needed or just display as is via the id and errorText functions.

Resolution of function names are run against the functionRegistry, as such you must register any UDF's or database functions _before_ calling validate.

## What if I want to actually test the ruleSuite runs?

```scala
def validate(schemaOrFrame: Either[StructType, DataFrame], ruleSuite: RuleSuite, showParams: ShowParams = ShowParams(), runnerFunction: Option[DataFrame => Column] = None, qualityName: String = "Quality", recursiveLambdasSOEIsOk: Boolean = false, transformBeforeShow: DataFrame => DataFrame = identity): (Set[RuleError], Set[RuleWarning], String, RuleSuiteDocs, Map[Id, ExpressionLookup])
```

Given you can either use a ruleRunner or a ruleEngineRunner and set a number of parameters on those Column functions the validate runnerFunction is as simple DataFrame => Column that allows you to tweak the output.  In the case of ruleEngineRunner you could use debug mode, try with different DDL output types etc.  Use the qualityName parameter if you want to store the output in another column.  If you don't provide the runnerFunction the resulting string will be empty.

You don't actually have to provide a DataFrame, instead using just schema will generate an empty dataset to allow Spark to resolve against.  Using a DataFrame parameter will allow you to capture the output in the resulting tuples _3 String.

There are a number of overloaded validate arity functions to help solve common cases, they all delegate to the above function, whic also returns the documentation objects for each expression in the RuleSuite via the RuleSuiteDocs object, this provides a base for the [documentation of a RuleSuite](expressionDocs.md).

## What I want to change the dataframe before I show it?

Using the transformBeforeShow parameter you can enhance, select or filter the DataFrame before showing it.

## Why do I get a java.lang.AbstractMethodError when validating?

The validation code also validates the sql documentation, checking documented parameters against lambda parameter names (or indeed that you have any parameters when not a lambda).
 
You probably have a dependency on the Scala Compiler, due to the scala compiler [requiring a different parser combinator library](https://github.com/scala/scala-parser-combinators/issues/197#issuecomment-480486554) this may occur due to classpath issues.

To remediate please make sure that Quality is higher up on your dependencies than the scala compiler is.  If need be manually specify the parser combinator library dependency, making sure to use the same version declared in Qualities pom. 

