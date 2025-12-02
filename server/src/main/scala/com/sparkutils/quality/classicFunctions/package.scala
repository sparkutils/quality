package com.sparkutils.quality

import com.sparkutils.quality.impl.bloom.parquet.{BlockSplitBloomFilterImports, BucketedCreatorFunctionImports}
import com.sparkutils.quality.impl.bloom.{BloomExpressionFunctions, BloomFilterLookupFunctionImport, BloomFilterLookupImports, BloomFilterRegistration, BloomFilterTypes}
import com.sparkutils.quality.impl.imports._
import com.sparkutils.quality.impl.mapLookup.{MapLookupFunctionImports, MapLookupImportsShared}
import com.sparkutils.quality.impl.util.{LookupIdFunctionsImports, SerializingImports}

/**
 * Collection of the Quality Spark Expressions for use in select( Column * )
 */
package object classicFunctions extends BloomFilterLookupFunctionImport
  with ClassicRuleRunnerImports with BloomExpressionFunctions with RuleFolderRunnerImports
  with MapLookupFunctionImports with BloomFilterTypes with BucketedCreatorFunctionImports with ClassicRuleRunnerFunctionsImport
  with BloomFilterRegistration with RuleRunnerImports with Serializable with MapLookupImportsShared with LookupIdFunctionsImports
  with BloomFilterLookupImports with BlockSplitBloomFilterImports with SerializingImports with LambdaFunctionsImports
  with ClassicRuleEngineRunnerImports with ValidationImports with ProcessDisableIfMissingImports {

}
