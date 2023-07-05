

## Errors Summary
|Type|Count|
|---|---:|
      | __SparkFunctionNameError__ Name map_Lookup is missing | 1 |
| __RuleNameError__ Name nonExistentFielda is missing | 1 |
| __RuleNameError__ Name nonExistentFieldb is missing | 1 |
| __SparkFunctionNameError__ Name nonExistentFunction is missing | 1 |

## Warnings Summary
|Type|Count|
|---|---:|
      | __NonLambdaDocParameters__ Parameter documentation is present on a non lambda expression | 2 |
| __ExtraDocParameter__ Parameter fielda is not found in the lambda expression | 1 |

## Errors Identified for RuleSuite - Id 0, 1 <a name="ruleSuiteErrors_0_1"></a>

### Id 5, 1 <a name="Errors_0_1"></a>

__SparkFunctionNameError__ Name map_Lookup is missing occurred when processing id Id(5,1) against <a href="../sampleDocsOutput/#rule_5_1">expression</a>

```sql
map_Lookup(fielda, fieldb) and test(fieldb)
```



### Id 16, 1 <a name="Errors_0_1"></a>

__RuleNameError__ Name nonExistentFielda is missing occurred when processing id Id(16,1) against <a href="../sampleDocsOutput/#rule_16_1">expression</a>

```sql
nonExistentFunction(fielda) and nonExistentFielda > nonExistentFieldb
```



__RuleNameError__ Name nonExistentFieldb is missing occurred when processing id Id(16,1) against <a href="../sampleDocsOutput/#rule_16_1">expression</a>

```sql
nonExistentFunction(fielda) and nonExistentFielda > nonExistentFieldb
```



__SparkFunctionNameError__ Name nonExistentFunction is missing occurred when processing id Id(16,1) against <a href="../sampleDocsOutput/#rule_16_1">expression</a>

```sql
nonExistentFunction(fielda) and nonExistentFielda > nonExistentFieldb
```




## Warnings Identified for RuleSuite - Id 0, 1 <a name="ruleSuiteWarnings_0_1"></a>

### Id 2, 1 <a name="Warnings_0_1"></a>

__NonLambdaDocParameters__ Parameter documentation is present on a non lambda expression, occurred when processing id Id(2,1) against <a href="../sampleDocsOutput/#rule_2_1">expression</a>

```sql
/** description @param fielda desc */ concat(fielda, fieldb)
```



### Id 1002, 1 <a name="Warnings_0_1"></a>

__NonLambdaDocParameters__ Parameter documentation is present on a non lambda expression, occurred when processing id Id(1002,1) against <a href="../sampleDocsOutput/#outputExpression_1002_1">expression</a>

```sql
/** description 2 @param fielda desc 2 */ concat(fielda, fieldb)
```



### Id 6, 1 <a name="Warnings_0_1"></a>

__ExtraDocParameter__ Parameter fielda is not found in the lambda expression, occurred when processing id Id(6,1) against <a href="../sampleDocsOutput/#lambda_6_1">expression</a>

```sql
/** lambda description @param fielda lambda desc */ variable -> variable
```



