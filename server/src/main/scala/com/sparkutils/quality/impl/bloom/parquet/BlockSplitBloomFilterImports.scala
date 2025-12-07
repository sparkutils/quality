package com.sparkutils.quality.impl.bloom.parquet

trait BlockSplitBloomFilterImports {

  /**
   * Calculate optimal size according to the number of distinct values and false positive probability.
   *
   * @param n : The number of distinct values.
   * @param p : The false positive probability.
   * @return optimal number of bits of given n and p.
   */
  def optimalNumOfBits(n: Long, p: Double): Int = BlockSplitBloomFilter.optimalNumOfBits(n, p)

  def optimalNumberOfBuckets(n: Long, p: Double): Long = BlockSplitBloomFilter.optimalNumberOfBuckets(n, p)

}
