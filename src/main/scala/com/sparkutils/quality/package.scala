package com.sparkutils

import com.sparkutils.quality.impl.bloom.parquet.{BlockSplitBloomFilterImports, BucketedCreatorFunctionImports}
import com.sparkutils.quality.impl.bloom.{BloomFilterLookupImports, BloomFilterRegistration, BloomFilterTypes}
import com.sparkutils.quality.impl.id.{GenericLongBasedImports, GuaranteedUniqueIDImports}
import com.sparkutils.quality.impl.mapLookup.{MapLookupFunctions, MapLookupImports}
import com.sparkutils.quality.impl.util.ComparableMapsImports
import com.sparkutils.quality.impl.views.ViewLoading
import com.sparkutils.quality.impl.{ProcessDisableIfMissingImports, RuleEngineRunnerImports, RuleFolderRunnerImports, RuleRunnerFunctionsImport, RuleRunnerImports, RuleSparkTypes, Validation, ValidationImports}
import com.sparkutils.quality.utils.{AddDataFunctions, AddDataFunctionsImports, LookupIdFunctionsImports, SerializingImports}
import org.apache.spark.sql.internal.SQLConf
import org.slf4j.LoggerFactory

/**
 * Provides an easy import point for the library.
 */
package object quality extends BloomFilterTypes with BucketedCreatorFunctionImports with RuleRunnerFunctionsImport
  with BloomFilterRegistration with RuleRunnerImports with Serializable with MapLookupImports
  with RuleSparkTypes with BloomFilterLookupImports with BlockSplitBloomFilterImports with SerializingImports
  with AddDataFunctionsImports with LambdaFunctionsImports with LookupIdFunctionsImports
  with GenericLongBasedImports with GuaranteedUniqueIDImports with RuleEngineRunnerImports with ValidationImports
  with ProcessDisableIfMissingImports with RuleFolderRunnerImports with ComparableMapsImports with ViewLoading {
  // NB it must inherit Serializable due to the nested types and sparks serialization

  val logger = LoggerFactory.getLogger("com.sparkutils.quality")

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

  def debugTime[T](what: String, log: (Long, String)=>Unit = (i, what) => {logger.debug(s"----> ${i}ms for $what")} )( thunk: => T): T = {
    val start = System.currentTimeMillis
    try {
      thunk
    } finally {
      val stop = System.currentTimeMillis

      log(stop - start, what)
    }
  }

  /**
   * first attempts to get the system env, then system java property then sqlconf
   * @param name
   * @return
   */
  def getConfig(name: String) = try {
    val res = System.getenv(name)
    if (res ne null)
      res
    else {
      val sp = System.getProperty(name)
      if (sp ne null)
        sp
      else
        SQLConf.get.getConfString(name, "")
    }
  } catch {
    case _: Throwable => ""
  }

}

