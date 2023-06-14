package com.sparkutils.quality.impl.bloom.parquet

import com.sparkutils.quality.BloomModel
import org.apache.spark.sql.DataFrame

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
  def bloomFrom(dataFrame: DataFrame, bloomOn: String, expectedSize: Int,
                fpp: Double = 0.01, bloomId: String = java.util.UUID.randomUUID().toString, partitions: Int = 0): BloomModel =
    BucketedCreator.bloomFrom(dataFrame, bloomOn, expectedSize, fpp, bloomId, partitions)

  /**
   * Where the bloom filter files will be stored
   */
  val bloomFileLocation = BucketedCreator.bloomFileLocation
}
