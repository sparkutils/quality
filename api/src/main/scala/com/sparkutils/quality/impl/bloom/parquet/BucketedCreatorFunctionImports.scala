package com.sparkutils.quality.impl.bloom.parquet

import com.sparkutils.quality.BloomModel
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{Column, DataFrame}

trait BucketedCreatorFunctionImports {
  /**
   * Generates a bloom filter using the expression passed via bloomOn with optional repartitioning and a default fpp of 0.01.
   * A unique run will be created so other results can co-exist but this can be overridden by specifying bloomId.
   * The other interim result files will be removed and the resulting BucketedFiles can be used in lookups or stored elsewhere.
   *
   * @param dataFrame
   * @param bloomOn a compatible expression to generate the bloom hashes against
   * @param expectedSize
   * @param fpp
   * @param bloomId
   * @param partitions
   * @return
   */
  def bloomFrom(dataFrame: DataFrame, bloomOn: Column, expectedSize: Int,
                fpp: Double = 0.01, bloomId: String = java.util.UUID.randomUUID().toString, partitions: Int = 0): BloomModel =
    BucketedCreator.bloomFrom(dataFrame, bloomOn, expectedSize, fpp, bloomId, partitions)

  /**
   * Generates a bloom filter using the expression passed via bloomOn with optional repartitioning and a default fpp of 0.01.
   * A unique run will be created so other results can co-exist but this can be overridden by specifying bloomId.
   * The other interim result files will be removed and the resulting BucketedFiles can be used in lookups or stored elsewhere.
   *
   * @param dataFrame
   * @param bloomOn a compatible expression to generate the bloom hashes against
   * @param expectedSize
   * @param fpp
   * @param bloomId
   * @param partitions
   * @return
   */
  def bloomFrom(dataFrame: DataFrame, bloomOn: String, expectedSize: Int): BloomModel =
    BucketedCreator.bloomFrom(dataFrame, expr(bloomOn), expectedSize)

  /**
   * Generates a bloom filter using the expression passed via bloomOn with optional repartitioning and a default fpp of 0.01.
   * A unique run will be created so other results can co-exist but this can be overridden by specifying bloomId.
   * The other interim result files will be removed and the resulting BucketedFiles can be used in lookups or stored elsewhere.
   *
   * @param dataFrame
   * @param bloomOn a compatible expression to generate the bloom hashes against
   * @param expectedSize
   * @param fpp
   * @param bloomId
   * @param partitions
   * @return
   */
  def bloomFrom(dataFrame: DataFrame, bloomOn: String, expectedSize: Int, fpp: Double): BloomModel =
    BucketedCreator.bloomFrom(dataFrame, expr(bloomOn), expectedSize, fpp)


  /**
   * Generates a bloom filter using the expression passed via bloomOn with optional repartitioning and a default fpp of 0.01.
   * A unique run will be created so other results can co-exist but this can be overridden by specifying bloomId.
   * The other interim result files will be removed and the resulting BucketedFiles can be used in lookups or stored elsewhere.
   *
   * @param dataFrame
   * @param bloomOn a compatible expression to generate the bloom hashes against
   * @param expectedSize
   * @param fpp
   * @param bloomId
   * @param partitions
   * @return
   */
  def bloomFrom(dataFrame: DataFrame, bloomOn: String, expectedSize: Int,
                fpp: Double, bloomId: String): BloomModel =
    BucketedCreator.bloomFrom(dataFrame, expr(bloomOn), expectedSize, fpp, bloomId)

  /**
   * Generates a bloom filter using the expression passed via bloomOn with optional repartitioning and a default fpp of 0.01.
   * A unique run will be created so other results can co-exist but this can be overridden by specifying bloomId.
   * The other interim result files will be removed and the resulting BucketedFiles can be used in lookups or stored elsewhere.
   *
   * @param dataFrame
   * @param bloomOn a compatible expression to generate the bloom hashes against
   * @param expectedSize
   * @param fpp
   * @param bloomId
   * @param partitions
   * @return
   */
  def bloomFrom(dataFrame: DataFrame, bloomOn: String, expectedSize: Int,
                fpp: Double, bloomId: String, partitions: Int): BloomModel =
    BucketedCreator.bloomFrom(dataFrame, expr(bloomOn), expectedSize, fpp, bloomId, partitions)

  /**
   * Where the bloom filter files will be stored
   */
  val bloomFileLocation = BucketedCreator.bloomFileLocation
}
