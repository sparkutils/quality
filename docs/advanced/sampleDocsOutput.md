
## RuleSuite Id 0, 1 <a name="ruleSuite_0_1"></a> - <span class="rule_error">3 Errors</span><span class="rule_warning"> 3 Warnings</span>



### RuleSet Id - 1, 1 <a name="ruleSet_1_1"></a>

#### Rule Id - 2, 1 <a name="rule_2_1"></a> -  <a href="../sampleDocsValidation/#Warnings_2_1" class="rule_warning">1 Warnings</a>
description

|Parameter|Description|
|---|---|
|fielda|desc|



    

```sql
concat(fielda, fieldb)
```

<div class="spark_functions_used comma-list">
<p>Spark functions used:</p>
<ul>

<li> <a target="_blank" href="https://spark.apache.org/docs/latest/api/sql/index.html#concat">concat</a> </li>
</ul>
</div>



__Triggers__ output rule with id <a href="#outputRule_6_1">6, 1</a> _Salience_ 0
 
#### Rule Id - 6, 1 <a name="rule_6_1"></a> -  <a href="../sampleDocsValidation/#Warnings_6_1" class="rule_warning">1 Warnings</a>




    

```sql
fielda > fieldb
```



__Triggers__ output rule with id <a href="#outputRule_1002_1">1002, 1</a> _Salience_ 0
 
#### Rule Id - 4, 1 <a name="rule_4_1"></a> - 




    

```sql
testCaller2(fielda > fieldb) and test(fieldb)
```

<div class="lambdas_used comma-list">
<p>Lambda used:</p>
<ul>

<li> <a href="#lambda_7_2">testCaller2</a> </li>

<li> <a href="#lambda_6_1">test</a> </li>
</ul>
</div>


 
#### Rule Id - 5, 1 <a name="rule_5_1"></a> - 




    

```sql
map_Lookup(fielda, fieldb) and test(fieldb)
```

<div class="spark_functions_used comma-list">
<p>Quality functions used:</p>
<ul>

<li> <a target="_blank" href="../../sqlfunctions/#map_lookup">map_Lookup</a> </li>
</ul>
</div>

<div class="lambdas_used comma-list">
<p>Lambda used:</p>
<ul>

<li> <a href="#lambda_6_1">test</a> </li>
</ul>
</div>


 
#### Rule Id - 16, 1 <a name="rule_16_1"></a> -  <a href="../sampleDocsValidation/#Errors_16_1" class="rule_error">3 Errors</a>




    

```sql
nonExistentFunction(fielda) and nonExistentFielda > nonExistentFieldb
```


 



## Output Rules


### Output Rule Id - 6, 1 <a name="outputRule_6_1"></a> -  <a href="../sampleDocsValidation/#Warnings_6_1" class="rule_warning">1 Warnings</a>




    

```sql
testCaller2(fielda, fieldb)
```

<div class="lambdas_used comma-list">
<p>Lambda used:</p>
<ul>

<li> <a href="#lambda_7_2">testCaller2</a> </li>
</ul>
</div>



<div class="lambdas_used comma-list">
<p>Called by Rules:</p>
<ul>

<li> <a href="#rule_2_1">2 - 1</a> </li>
</ul>
</div>



### Output Rule Id - 1002, 1 <a name="outputRule_1002_1"></a> -  <a href="../sampleDocsValidation/#Warnings_1002_1" class="rule_warning">1 Warnings</a>
description 2

|Parameter|Description|
|---|---|
|fielda|desc 2|



    

```sql
concat(fielda, fieldb)
```

<div class="spark_functions_used comma-list">
<p>Spark functions used:</p>
<ul>

<li> <a target="_blank" href="https://spark.apache.org/docs/latest/api/sql/index.html#concat">concat</a> </li>
</ul>
</div>



<div class="lambdas_used comma-list">
<p>Called by Rules:</p>
<ul>

<li> <a href="#rule_6_1">6 - 1</a> </li>
</ul>
</div>



## Lambdas


## Lambda testCaller2
    
### Rule - Id - 7, 2 <a name="lambda_7_2"></a> - 
__Name__ testCaller2




    

```sql
(outervariable1, variable2) -> concat(outervariable1, variable2) and acos(fielda)
```

<div class="spark_functions_used comma-list">
<p>Spark functions used:</p>
<ul>

<li> <a target="_blank" href="https://spark.apache.org/docs/latest/api/sql/index.html#concat">concat</a> </li>

<li> <a target="_blank" href="https://spark.apache.org/docs/latest/api/sql/index.html#acos">acos</a> </li>
</ul>
</div>



<div class="lambdas_used comma-list">
<p>Called by Lambdas:</p>
<ul>

<li> <a href="#lambda_8_1">testCaller3</a> </li>
</ul>
</div>

<div class="lambdas_used comma-list">
<p>Called by Output Expressions:</p>
<ul>

<li> <a href="#outputRule_6_1">6 - 1</a> </li>
</ul>
</div>

<div class="lambdas_used comma-list">
<p>Called by Rules:</p>
<ul>

<li> <a href="#rule_4_1">4 - 1</a> </li>
</ul>
</div>




## Lambda testCaller3
    
### Rule - Id - 8, 1 <a name="lambda_8_1"></a> - 
__Name__ testCaller3
lambda description only



    

```sql
(outervariable1, variable2, variable3) -> testCaller2(outervariable1, variable2)
```

<div class="lambdas_used comma-list">
<p>Lambda used:</p>
<ul>

<li> <a href="#lambda_7_2">testCaller2</a> </li>
</ul>
</div>






## Lambda test
    
### Rule - Id - 6, 1 <a name="lambda_6_1"></a> -  <a href="../sampleDocsValidation/#Warnings_6_1" class="rule_warning">1 Warnings</a>
__Name__ test
lambda description

|Parameter|Description|
|---|---|
|fielda|lambda desc|



    

```sql
variable -> variable
```



<div class="lambdas_used comma-list">
<p>Called by Rules:</p>
<ul>

<li> <a href="#rule_5_1">5 - 1</a> </li>

<li> <a href="#rule_4_1">4 - 1</a> </li>
</ul>
</div>




