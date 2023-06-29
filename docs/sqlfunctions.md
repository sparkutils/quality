---
functions:
  rule_result:
    description: |
      rule_result(ruleSuiteResultColumn, packedRuleSuiteId, packedRuleSetId, packedRuleId) uses the long id's to retrieve the integer ruleResult (see below for ExpressionRunner) or null if it can't be found.  

      You can use pack_ints(id, version) to specify each id if you don't already have the packed long version.  This is suitable for retrieving individual rule results, for example to aggregate counts of a specific rule result, without having to resort to using filter and map values.

      rule_result works with ruleRunner (DQ) results (including details) and ExpressionRunner results.  ExpressionRunner results return a tuple of ruleResult and resultDDL, both strings, or if strip_result_ddl is called a string.
    tags:
      - rule
  strip_result_ddl:
    description: |
      strip_result_ddl(expressionsResult) removes the resultDDL field from expressionsRunner results, leaving only the string result itself for more compact storage 
    tags:
      - rule
  murmur3_ID:
    description: "murmur3ID('prefix', fields*) Generates a 160bit id using murmer3 hashing over input fields, prefix is used with the _base, _i0 and _i1 fields in the resulting structure"
    tags:
      - ID
      - Hash
  unique_ID:
    description: "uniqueID('prefix') Generates a 160bit guaranteed unique id (requires MAC address uniqueness) with contiguous higher values within a partition and overflow with timestamp ms., prefix is used with the _base, _i0 and _i1 fields in the resulting structure"
    tags:
      - ID
  id_size:
    description: "id_size(base64) Given a base64 from id_base64 returns the number of _i long fields"
    tags:
      - ID
  id_base64:
    description: "id_base64(base, i0, i1, ix) Generates a base64 encoded representation of the id, either the single struct field or the individual parts"
    alternatives:
      - "id_base64(id_struct) Uses an id field to generate"         
    tags:
      - ID      
  id_from_base64:
    description: "id_from_base64(base64) Parses the base64 string with an expected default long size of two i.e. an 160bit ID, any string which is not of the correct size will return null"
    alternatives:
      - "id_from_base64(base64f, size) Uses a size, which must be literal, to specify the type"
    tags:
      - ID
  id_raw_type:
    description: "id_raw_type(idstruct) Given a prefixed id returns the fields without their prefix"
    tags:
      - ID            
  rng_ID:
    description: "rng_ID('prefix') Generates a 160bit random id using XO_RO_SHI_RO_128_PP, prefix is used with the _base, _i0 and _i1 fields in the resulting structure"
    alternatives:
      - "rng_Id('prefix', 'algorithm') Uses Commons RNG RandomSource to implement the RNG, using other algorithm's may generate more long _iN fields"
      - "rng_Id('prefix', 'algorithm', seedL) Uses Commons RNG RandomSource to implement the RNG with a long seed, using other algorithm's may generate more long _iN fields"
    tags:
      - ID
      - RNG
  provided_ID:
    description: "provided_ID('prefix', existingLongs) creates an id for an existing array of longs, prefix is used with the _base, _i0 and _iN fields in the resulting structure"
    tags:
      - ID
  field_Based_ID:
    description: "field_Based_ID('prefix', 'digestImpl', fields*) creates a variable bit length id by using a given MessageDigest impl over the fields, prefix is used with the _base, _i0 and _iN fields in the resulting structure"
    tags:
      - ID
      - Hash
  hash_Field_Based_ID:
    description: "hash_Field_Based_ID('prefix', 'digestImpl', fields*) creates a variable bit length id by using a given Guava Hasher impl over the fields, prefix is used with the _base, _i0 and _iN fields in the resulting structure"
    tags:
      - ID
      - Hash
  za_Longs_Field_Based_ID:
    description: "za_Longs_Field_Based_ID('prefix', 'digestImpl', fields*) creates a variable length id by using a given Zero Allocation impl over the fields, prefix is used with the _base, _i0 and _iN fields in the resulting structure. Murmur3_128 is faster than on the Guava implementation."
    tags:
      - ID
      - Hash
  za_Field_Based_ID:
    description: |
      za_Field_Based_ID('prefix', 'digestImpl', fields*) creates a 64bit id (96bit including header) by using a given Zero Allocation impl over the fields, prefix is used with the _base and _i0 fields in the resulting structure.

      Prefer using the zaLongsFieldBasedID for less collisions
    tags:
      - ID
      - Hash
  digest_To_Longs_Struct:
    description: "digest_To_Longs_Struct('digestImpl', fields*) creates structure of longs with i0 to iN named fields based on creating the given MessageDigest impl."
    tags:
      - Hash
  digest_To_Longs:
    description: "digest_To_Longs('digestImpl', fields*) creates an array of longs based on creating the given MessageDigest impl.  A 128-bit impl will generate two longs from it's digest"
    tags:
      - Hash
  rule_Suite_Result_Details:
    description: "rule_Suite_Result_Details(dq) strips the overallResult from the dataquality results, suitable for keeping overall result as a top-level field with associated performance improvements"
  id_Equal:
    description: "id_Equal(leftPrefix, rightPrefix) takes two prefixes which will be used to match leftPrefix_base = rightPrefix_base, i0 and i1 fields.  It does not currently support more than two i's "
    tags:
      - ID
  long_Pair_Equal:
    description: "long_Pair_Equal(leftPrefix, rightPrefix) takes two prefixes which will be used to match leftPrefix_lower = rightPrefix_lower and leftPrefix_higher = rightPrefix_higher"
    tags:
      - longs
  big_Bloom:
    description: |
      big_Bloom(buildFrom, expectedSize, expectedFPP) creates an aggregated bloom filter using the buildFrom expression.  
      
      The blooms are stored on a shared filesystem using a generated uuid, they can scale to high numbers of items whilst keeping the FPP (e.g. millions at 0.01 would imply 99% probability, you may have to cast to double in Spark 3.2).
      
      buildFrom can be driven by digestToLongs or hashWith functions when using multiple fields.
    alternatives:
      - "big_Bloom(buildFrom, expectedSize, expectedFPP, 'bloom_loc') - per above but uses a fixed string bloom_loc instead of a uuid"
    tags:
      - bloom
  small_Bloom:
    description: "small_Bloom(buildFrom, expectedSize, expectedFPP) creates a simply bytearray bloom filter using the expected size and fpp - 0.01 is 99%, you may have to cast to double in Spark 3.2. buildFrom can be driven by digestToLongs or hashWith functions when using multiple fields."
    tags:
      - bloom
  long_Pair_From_UUID:
    description: "long_Pair_From_UUID(expr) converts a UUID to a structure with lower and higher longs"
    tags:
      - longs
  long_Pair:
    description: "long_Pair(lower, higher) creates a structure with these lower and higher longs"
    tags:
      - longs
  rng_UUID:
    description: "rng_UUID(expr) takes either a structure with lower and higher longs or a 128bit binary type and converts to a string uuid"
    tags:
      - longs
  rng:
    description: "rng() Generates a 128bit random id using XO_RO_SHI_RO_128_PP, encoded as a lower and higher long pair"
    alternatives:
      - "rng('algorithm') Uses Commons RNG RandomSource to implement the RNG"
      - "rng('algorithm', seedL) Uses Commons RNG RandomSource to implement the RNG with a long seed"
    tags:
      - longs
      - RNG
  rng_Bytes:
    description: "rng_Bytes() Generates a 128bit random id using XO_RO_SHI_RO_128_PP, encoded as a byte array"
    alternatives:
      - "rng_Bytes('algorithm') Uses Commons RNG RandomSource to implement the RNG"
      - "rng_Bytes('algorithm', seedL) Uses Commons RNG RandomSource to implement the RNG with a long seed"
      - "rng_Bytes('algorithm', seedL, byteCount) Uses Commons RNG RandomSource to implement the RNG with a long seed, with a specific byte length integer (e.g. 16 is two longs, 8 is integer)"
    tags:
      - RNG
  return_Sum:
    description: "return_Sum( sum type ddl ) just returns the sum and ignores the count param, expands to resultsWith( [sum ddl_type], (sum, count) -> sum)"
    tags:
      - aggregate
  sum_With:
    description: "sum_With( x ) adds expression x for each row processed in an aggExpr with a default of LongType"
    alternatives:
      - "sum_With( [ddl type], x) Use the given ddl type e.g. 'MAP&lt;STRING, DOUBLE&gt;'"
    tags:
      - aggregate
  results_With:
    description: "results_With( x ) process results lambda x (e.g. (sum, count) -> sum ) that takes sum from the aggregate, count from the number of rows counted.  Defaults both the sumtype and counttype as LongType"
    alternatives:
      - "results_With( [sum ddl type], x) Use the given ddl type for the sum type e.g. 'MAP&lt;STRING, DOUBLE&gt;'"
      - "results_With( [sum ddl type], [result ddl type], x) Use the given ddl type for the sum and result types"
    tags:
      - aggregate
  inc:
    description: "inc() increments the current sum by 1"
    alternatives:
      - "inc( x ) use an expression of type Long to increment"
    tags:
      - aggregate
  meanF:
    description: "meanF() simple mean on the results, expecting sum and count type Long"
    tags:
      - aggregate
  agg_Expr:
    description: |
      agg_Expr( [ddl sum type], filter, sum, result) aggregates on rows which match the filter expression using the sum expression to aggregate then processes the results using the result expression.
      
      You can run multiple agg_Expr's in a single pass select, use the first parameter to thread DDL type information through to the sum and result functions.
    tags:
      - aggregate
  passed:
    description: "passed() returns the Passed Integer for use in filtering: 10000"
    tags:
      - rule
  failed:
    description: "failed() returns the Failed Integer result (0) for use in filtering"
    tags:
      - rule
  soft_Failed:
    description: "soft_Failed() returns the SoftFailed Integer result (-1) for use in filtering"
    tags:
      - rule
  disabled_Rule:
    description: "disabledRule() returns the DisabledRule Integer result (-2) for use in filtering and to disable rules (which may not signify a version bump)"
    tags:
      - rule
  coalesce_If_Attributes_Missing_Disable:
    description: |
      coalesce_If_Attributes_Missing_Disable(expr) substitutes expr with the DisabledRule Integer result (-2) when expr has missing attributes in the source dataframe.  Your code must call the scala processIfAttributeMissing function before using in validate or ruleEngineRunner/ruleRunner:
      
      ```scala
      val missingAttributesAreReplacedRS = processIfAttributeMissing(rs, struct)

      val (errors, _) = validate(struct, missingAttributesAreReplacedRS)

      // use it missingAttributesAreReplacedRS in your dataframe..
      ```
    tags:
      - rule
  coalesce_If_Attributes_Missing:
    description: |
      coalesce_If_Attributes_Missing(expr, replaceWith) substitutes expr with the replaceWith expression when expr has missing attributes in the source dataframe.    Your code must call the scala processIfAttributeMissing function before using in validate or ruleEngineRunner/ruleRunner:

      ```scala
      val missingAttributesAreReplacedRS = processIfAttributeMissing(rs, struct)

      val (errors, _) = validate(struct, missingAttributesAreReplacedRS)

      // use it missingAttributesAreReplacedRS in your dataframe..
      ```
    tags:
      - rule
  pack_Ints:
    description: "pack_Ints(lower, higher) a packaged long from two ints, used within result compression"
    tags:
      - ruleid
  unpack:
    description: "unpack(expr) takes a packed rule long and unpacks it to a .id and .version structure"       
    tags:
      - ruleid
  unpack_Id_Triple:
    description: "unpack_Id_Triple(expr) takes a packed rule triple of longs (ruleSuiteId, ruleSetId and ruleId) and unpacks it to (ruleSuiteId, ruleSuiteVersion, ruleSetId, ruleSetVersion, ruleId, ruleVersion)"       
    tags:
      - ruleid
  soft_Fail:
    description: "soft_Fail(ruleexpr) will treat any rule failure (e.g. failed() ) as returning softFailed()"
    tags:
      - rule
  probability:
    description: "probability(expr) will translate probability rule results into a double, e.g. 1000 returns 0.01. This is useful for interpreting and filtering on probability based results: 0 -> 10000 non-inclusive"
    tags:
      - rule
  flatten_Results:
    description: "flatten_Results(dataQualityExpr) expands data quality results into a flat array"
  flatten_Rule_Results:
    description: |
      flatten_Rule_Results(dataQualityExpr) expands data quality results into a structure of flattenedResults, salientRule (the one used to create the output) and the rule result.
      
      salientRule will be null if there was no matching rule
  probability_In:
    description: |
      probability_In(expr, 'bloomid') returns the probability of the expr being in the bloomfilter specified by bloomid.  
      
      This function either returns 0.0, where it is definitely not present, or the original FPP where it _may_ be present.
      
      You may use digestToLongs or hashWith as appropriate to use multiple columns safely.
    tags:
      - bloom
  map_Lookup:
    description: "map_Lookup(expr, 'mapid') returns either the lookup in map specified by mapid or null"
    tags:
      - map
  map_Contains:
    description: "map_Contains(expr, 'mapid') returns true if there is an item in the map"
    tags:
      - map
  comparable_Maps:
    description: | 
      comparable_Maps(struct | array | map) converts any maps in the input param into sorted arrays of a key, value struct.
      
      This allows developers to perform sorts, distincts, group bys and union set operations with Maps, currently not supported by Spark sql as of 3.4.
            
      The sorting behaviour uses Sparks existing odering logic but allows for extension during the calls to the registerQualityFunctions via the mapCompare parameter and the defaultMapCompare function.
    tags:
      - map
  reverse_Comparable_Maps:
    description: "reverses a call to comparableMaps"
    tags:
      - map
  hash_With_Struct:
    description: "per hash_With('HASH', fields*) but generates a struct with i0 to ix named longs.  This structure is not suitable for blooms"
    tags:
      - Hash
  za_Hash_Longs_With_Struct:
    description: "similar to za_Hash_Longs_With('HASH', fields*) but generates an ID relevant multi length long struct, which is not suitable for blooms"
    tags:
      - Hash
  za_Hash_With:
    description: |
      za_Hash_With('HASH', fields*) generates a single length long array always with 64 bits but with a [zero allocation implementation](https://github.com/OpenHFT/Zero-Allocation-Hashing).  This structure is suitable for blooms, the default XX algorithm is used by the internal bigBloom implementation.
      
      Available HASH functions are MURMUR3_64, CITY_1_1, FARMNA, FARMOU, METRO, WY_V3, XX
    tags:
      - Hash
  za_Hash_With_Struct:
    description: |
      similar to za_Hash_With('HASH', fields*) but generates an ID relevant multi length long struct (of one long), which is not suitable for blooms.

      Prefer zaHashLongsWithStruct for reduced collisions with either the MURMUR3_128 or XXH3 versions of hashes
    tags:
      - Hash
  za_Hash_Longs_With:
    description: |
      za_Hash_Longs_With('HASH', fields*) generates a multi length long array but with a [zero allocation implementation](https://github.com/OpenHFT/Zero-Allocation-Hashing).  This structure is suitable for blooms, the default XXH3 algorithm is the 128bit version of that used by the internal bigBloom implementation.
      
      Available HASH functions are MURMUR3_128, XXH3
    tags:
      - Hash
  hash_With:
    description: |
      hash_With('HASH', fields*) Generates a hash value (array of longs) suitable for using in blooms based on the given Guava hash implementation.
      
      *Note* based on testing the digestToLongs function for SHA256 and MD5 are faster.  
      
      Valid hashes: MURMUR3_32, MURMUR3_128, MD5, SHA-1, SHA-256, SHA-512, ADLER32, CRC32, SIPHASH24. When an invalid HASH name is provided MURMUR3_128 will be chosen.
      
      ??? warning "Open source Spark 3.1.2/3 issues"
          On Spark 3.1.2/3 open source this may get resolver errors due to a downgrade on guava version - 15.0 is used on Databricks, open source 3.0.3 uses 16.0.1, 3.1.2 drops this to 11 and misses crc32, sipHash24 and adler32.
    tags:
      - Hash      
  prefixed_To_Long_Pair:
    description: |
      prefixed_To_Long_Pair(field, 'prefix') converts a 128bit longpair field with the given prefix into a higher and lower long pair without prefix.
  
      This is suitable for converting provided id's into uuids for example via a further call to rngUUID.   
    tags:
      - ID, longs
  as_uuid:
    description: "as_uuid(lower_long, higher_long) converts two longs into a uuid, equivalent to rngUUID(longPair(lower, higher))"
    tags:
      - longs
  update_Field:
    description: |
      update_Field(structure_expr, 'field.subfield', replaceWith, 'fieldN', replaceWithN) processes structures allowing you to replace sub items (think lens in functional programming) using the structure fields path name.

      This is wrapped an almost verbatim version of [Make Structs Easier' AddFields](https://raw.githubusercontent.com/fqaiser94/mse/master/src/main/scala/org/apache/spark/sql/catalyst/expressions/AddFields.scala)
    tags:
      - struct
  \_:
    description: |
      \_( [ddl type], [nullable] ) provides PlaceHolders for lambda functions to allow partial application, use them in place of actual values or expressions to either change arity or allow use in \_lambda\_.

      The default type is Long / Bigint, you will have to provide the types directly when using something else.  By default the placeholders are assumed to be nullable (i.e. true), you can use false to state the field should not be null.
    tags:
      - lambda
  \_lambda\_:
    description: |
      \_lambda\_( user function ) extracts the Spark LambdaFunction from a resolved user function, this must have the correct types expected by the Spark HigherOrderFunction they are parameters for.

      This allows using user defined functions and lambdas with in-built Spark HigherOrderFunctions
    tags:
      - lambda
  callFun:
    description: |
      callFun( user function lambda variable, param1, param2, ... paramN ) used within a lambda function it allows calling a lambda variable that contains a user function.

      Used from the top level sql it performs a similar function expecting either a full user function or a partially applied function, typically returned from another lambda user function.
    tags:
      - lambda
  print_Expr:
    description: |
      print_Expr( [msg], expr ) prints the expression tree via toString with an optional msg 

      The message is printed to the **driver** nodes std. output, often shown in notebooks as well.  To use with unit testing you may overwrite the writer function in registerQualityFunctions, 
      you should however use a top level object and var to write into (or stream).
  print_Code:
    description: |
      print_Code( [msg], expr ) prints the code generated by an expression, the value variable and the isNull variable and forwards eval calls / type etc. to the expression.

      The code is printed once per partition on the **executors** std. output.  You will have to check each executor to find the used nodes output.  To use with unit testing on a single host you may overwrite the writer function in registerQualityFunctions, 
      you should however use a top level object and var to write into (or stream), printCode will not be able to write to std out properly (spark redirects / captures stdout) or non top level objects (due to classloader / function instance issues).  Testing on other hosts
      without using stdout should do so to a shared file location or similar.

      !!! "information" It is not compatible with every expression
          Aggregate expressions like aggExpr or sum etc. won't generate code so they aren't compatible with printCode.

          \_lambda\_ is also incompatible with printCode both wrapping a user function and the \_lambda\_ function.  Similarly the \_() placeholder function cannot be wrapped.

          Any function expecting a specific signature like aggExpr or other HigherOrderFunctions like aggregate or filter are unlikely to support wrapped arguements.
---

{% macro divstart(clazz) -%}<t class="{{ clazz }}" >{%- endmacro %}
{% macro divend() -%}</t>{%- endmacro %}

{% for ref, descs in functions.items()|sort(attribute='0') %}

{% set tag = None %}
{% set class %}{% if descs.tags and descs.tags|length > 0 %}{% for tag in descs.tags %}{{ tag }} {% endfor %}{% endif %}{% endset %}

## {{ ref }}
{{ descs.description }}
{% if descs.alternatives and descs.alternatives|length > 0 %}
__Alternatives:__
{% for alter in descs.alternatives %}
<div class="alternative">
{{ alter }}
</div>
{% endfor %}
{%- endif %}
{% if descs.tags and descs.tags|length > 0 %}
{%- endif %}
{% endfor %}
