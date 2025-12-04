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
  with ClassicRuleRunnerImports with BloomExpressionFunctions with ClassicRuleFolderRunnerImports
  with MapLookupFunctionImports with BloomFilterTypes with BucketedCreatorFunctionImports with ClassicRuleRunnerFunctionsImport
  with BloomFilterRegistration with Serializable with MapLookupImportsShared with LookupIdFunctionsImports
  with BloomFilterLookupImports with BlockSplitBloomFilterImports with SerializingImports with LambdaFunctionsImports
  with ClassicRuleEngineRunnerImports with ValidationImports with ProcessDisableIfMissingImports {

  /**
   * Creates a bloom filter from an array of bytes using the default Parquet bloom filter implementation
   * @param bytes
   * @return
   */
  def bloomLookup(bytes: Array[Byte]): BloomLookup =
    com.sparkutils.quality.impl.bloom.parquet.ThreadSafeBloomLookupImpl(bytes)

  /**
   * Creates a very large bloom filter from multiple buckets of 2gb arrays backed by the default Parquet implementation
   * @param bucketedFiles
   * @return
   */
  def bloomLookup(bucketedFiles: BloomModel): BloomLookup =
    com.sparkutils.quality.impl.bloom.parquet.ThreadSafeBucketedBloomLookup(bucketedFiles)

}
