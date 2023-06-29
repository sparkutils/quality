package com.sparkutils

import com.sparkutils.quality.impl.RuleSparkTypes
import com.sparkutils.quality.impl.bloom.parquet.{BlockSplitBloomFilterImports, BucketedCreatorFunctionImports}
import com.sparkutils.quality.impl.bloom.{BloomFilterLookupImports, BloomFilterRegistration, BloomFilterTypes}
import com.sparkutils.quality.impl.imports._
import com.sparkutils.quality.impl.mapLookup.MapLookupImports
import com.sparkutils.quality.impl.util.{AddDataFunctionsImports, LookupIdFunctionsImports, SerializingImports}
import com.sparkutils.quality.impl.views.ViewLoading
import org.apache.spark.sql.internal.SQLConf
import org.slf4j.LoggerFactory

/**
 * Provides an easy import point for the library.
 */
package object quality extends BloomFilterTypes with BucketedCreatorFunctionImports with RuleRunnerFunctionsImport
  with BloomFilterRegistration with RuleRunnerImports with Serializable with MapLookupImports with LookupIdFunctionsImports
  with RuleSparkTypes with BloomFilterLookupImports with BlockSplitBloomFilterImports with SerializingImports
  with AddDataFunctionsImports with LambdaFunctionsImports with RuleEngineRunnerImports with ValidationImports
  with ProcessDisableIfMissingImports with RuleFolderRunnerImports with ViewLoading with ExpressionRunnerImports {
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
  def getConfig(name: String, default: String = "") = try {
    val res = System.getenv(name)
    if (res ne null)
      res
    else {
      val sp = System.getProperty(name)
      if (sp ne null)
        sp
      else
        SQLConf.get.getConfString(name, default)
    }
  } catch {
    case _: Throwable => default
  }

}

