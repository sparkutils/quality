## Overview and terms

QualityRules is a matching engine which applies match/trigger rules to a Dataframe and, when these rules evaluate to passed (i.e. they match or trigger) output sql is run.

Only one trigger rule may produce output, so salience is used as a tie-breaker, the lowest salience wins.

??? warning "Aim to have unique salience for tie-breaking"
    If you have multiple trigger rules with the same salience that both trigger the "winning" output chosen is non-deterministic, chose your salience wisely.  

An alternative way to think of this is the trigger rules are your if and the output expressions are the when, from a logic perspective it may be helpful to think of them as output verbs - when this is true do that.

## Suggested approach to QualityRules management

- Keep unrelated rules in their own RuleSuites, making things easier to reason about
- Make commonly used lambdas or output expressions global
- Use descriptive verbs for your output expressions
    - Keep duplication or complexity in lambdas
    - Only use fields that change as parameters to those lambdas
- Always _start_ with test data you want to match against _and_ your expected output
- Run all test cases for your RuleSuite for any change, don't assume because your rule worked that others won't stop working
- Use the [validation and documentation](validation.md) functionality to document your lambdas and verify you've not made simple mistakes - Spark errors aren't always easy to understand 

This could be visualised as such:

```plantuml title="QualityEngine New Rule and Testing Process"

'!pragma layout smetana
!include <C4/C4_Context>

title QualityEngine Rule Management

start

:Define new test row to match on;
:Define expected output rows;
:Create new matching rule;
:Create new output expression;
repeat :Run Test Suite;
if (Is new test row matched\n by the correct rule?) then (yes)
if (Does the new rule match \nany other rows in the test suite?) then (yes)
else (no)
if (Is the output expected?) then (yes)
stop
else (no)
:Refine output expression;
endif
endif
else (no)
if (Does any other rule match the test row?) then (yes)
:Using lower salience;
else (no)
endif
endif
:Make matching rule more specific;
repeat while (Matching test cases pass) is (no) not (yes)

stop
```

!!! info "Don't repeat yourself"
    If you are typing the same trigger rule, output expression or even lambda text repeatedly - make another lambda and consider making it global 