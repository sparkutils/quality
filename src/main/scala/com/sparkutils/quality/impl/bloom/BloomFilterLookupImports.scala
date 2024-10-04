package com.sparkutils.quality.impl.bloom

import com.sparkutils.quality.impl.RuleRegistrationFunctions.registerWithChecks
import com.sparkutils.quality.impl.bloom.parquet.{BucketedFilesRoot, FileRoot}
import com.sparkutils.quality.{DataFrameLoader, Id, RuleSuite}
import com.sparkutils.quality.impl.util.ConfigLoader
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.ShimUtils.{column, expression}
import org.apache.spark.sql.{Column, DataFrame, QualitySparkUtils, ShimUtils, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.functions.lit

trait BloomFilterRegistration {

  /**
   * Registers this bloom map and associates the probabilityIn sql expression against it
   * @param bloomFilterMap
   */
  def registerBloomMapAndFunction(bloomFilterMap: Broadcast[BloomExpressionLookup.BloomFilterMap]) {
    val funcReg = SparkSession.getActiveSession.get.sessionState.functionRegistry
    def register(name: String, argsf: Seq[Expression] => Expression, paramNumbers: Set[Int] = Set.empty, minimum: Int = -1) =
      registerWithChecks(ShimUtils.registerFunction(funcReg), name, argsf, paramNumbers, minimum)

    val f = (exps: Seq[Expression]) => BloomFilterLookupExpression(exps(0), exps(1), bloomFilterMap)
    register("probability_In", f, Set(2))
  }

}

trait BloomFilterLookupFunctionImport {

  /**
   * Lookup the value against a bloom filter from bloomMap with name bloomFilterName.  In line with the sql functions please migrate to probability_in
   *
   * @param bloomFilterName
   * @param lookupValue
   * @param bloomMap
   * @return
   */
  @deprecated(since="0.1.0", message="Please migrate to bloom_lookup")
  def bloomFilterLookup(lookupValue: Column, bloomFilterName: Column, bloomMap: Broadcast[BloomExpressionLookup.BloomFilterMap]): Column =
    BloomFilterLookup(lookupValue, bloomFilterName, bloomMap)

  /**
   * Lookup the value against a bloom filter from bloomMap with name filterName.
   *
   * @param filterName
   * @param lookupValue
   * @param bloomMap
   * @return
   */
  def probability_in(lookupValue: Column, filterName: String, bloomMap: Broadcast[BloomExpressionLookup.BloomFilterMap]): Column =
    BloomFilterLookup(lookupValue, lit(filterName), bloomMap)
}

trait BloomFilterLookupImports {

  /**
   * Identifies bloom ids before (or after) resolving for a given ruleSuite, use to know which bloom filters need to be loaded
   *
   * @param ruleSuite a ruleSuite full of expressions to check
   * @return The bloom id's used, for unresolved expression trees this may contain blooms which are not present in the bloom map
   */
  def getBlooms(ruleSuite: RuleSuite): Seq[String] = BloomFilterLookup.getBlooms(ruleSuite)

  import Serializing.{factory, bloomRowEncoder}

  /**
   * Loads map configurations from a given DataFrame for ruleSuiteId.  Wherever token is present loader will be called and the filter optionally applied.
   * @return A tuple of MapConfig's and the names of rows which had unexpected content (either token or sql must be present)
   */
  def loadBloomConfigs(loader: DataFrameLoader, viewDF: DataFrame,
                     ruleSuiteIdColumn: Column,
                     ruleSuiteVersionColumn: Column,
                     ruleSuiteId: Id,
                     name: Column,
                     token: Column,
                     filter: Column,
                     sql: Column,
                     bigBloom: Column,
                     value: Column,
                     numberOfElements: Column,
                     expectedFPP: Column
                    ): (Seq[BloomConfig], Set[String]) =
    ConfigLoader.loadConfigs[BloomConfig, BloomRow](
      loader, viewDF,
      ruleSuiteIdColumn,
      ruleSuiteVersionColumn,
      ruleSuiteId,
      name,
      token,
      filter,
      sql,
      bigBloom,
      value,
      numberOfElements,
      expectedFPP
    )

  /**
   * Loads map configurations from a given DataFrame.  Wherever token is present loader will be called and the filter optionally applied.
   * @return A tuple of MapConfig's and the names of rows which had unexpected content (either token or sql must be present)
   */
  def loadBloomConfigs(loader: DataFrameLoader, viewDF: DataFrame,
                     name: Column,
                     token: Column,
                     filter: Column,
                     sql: Column,
                     bigBloom: Column,
                     value: Column,
                     numberOfElements: Column,
                     expectedFPP: Column
                    ): (Seq[BloomConfig], Set[String]) =
    ConfigLoader.loadConfigs[BloomConfig, BloomRow](
      loader, viewDF,
      name,
      token,
      filter,
      sql,
      bigBloom,
      value,
      numberOfElements,
      expectedFPP
    )

  /**
   * Loads bloom maps ready to register from configuration
   * @param configs
   * @return
   */
  def loadBlooms(configs: Seq[BloomConfig]): BloomExpressionLookup.BloomFilterMap = Serializing.loadBlooms(configs)
}

trait BloomExpressionFunctions {
  /**
   * Creates a bloom over the bloomOver expression, with an expected number of rows and fpp to build.
   *
   * This bloom fit's in a single byte array, as such it's limited to the number of elements it can fit it and still maintain the fpp, use big_bloom where the item counts are high and fpp must hold.
   *
   * @param bloomOver
   * @param expectedNumberOfRows
   * @param expectedFPP
   * @return
   */
  def small_bloom(bloomOver: Column, expectedNumberOfRows: Column, expectedFPP: Column): Column =
    column( new ParquetAggregator(expression(bloomOver), expression(expectedNumberOfRows), expression(expectedFPP))
      .toAggregateExpression())

  /**
   * Creates a bloom over the bloomOver expression, with an expected number of rows and fpp to build.
   *
   * This bloom fit's in a single byte array, as such it's limited to the number of elements it can fit it and still maintain the fpp, use big_bloom where the item counts are high and fpp must hold.
   *
   * @param bloomOver
   * @param expectedNumberOfRows
   * @param expectedFPP
   * @return
   */
  def small_bloom(bloomOver: Column, expectedNumberOfRows: Long, expectedFPP: Double): Column =
    small_bloom(bloomOver, lit(expectedNumberOfRows), lit(expectedFPP))

  /**
   * Creates a bloom over the bloomOver expression, with an expected number of rows and fpp to build.
   *
   * This bloom is an array of byte arrays, with the underlying arrays stored on file, as such it is only limited by file system size.
   *
   * @param bloomOver
   * @param expectedNumberOfRows
   * @param expectedFPP
   * @param id - the id of this bloom used to separate from other big_blooms - defaults to a uuid, this cannot refer to a row column or expression
   * @param bucketedFilesRoot - provide this to override the default file root for the underlying arrays - defaults to a temporary file location or /dbfs/ on Databricks.  The config property sparkutils.quality.bloom.root can also be used to set for all blooms in a cluster.
   * @return
   */
  def big_bloom(bloomOver: Column, expectedNumberOfRows: Column, expectedFPP: Column, id: Column = lit(java.util.UUID.randomUUID().toString), bucketedFilesRoot: BucketedFilesRoot = BucketedFilesRoot(FileRoot(com.sparkutils.quality.bloomFileLocation))): Column =
    column( BucketedArrayParquetAggregator(expression(bloomOver), expression(expectedNumberOfRows), expression(expectedFPP), expression(id), bucketedFilesRoot = bucketedFilesRoot )
      .toAggregateExpression())


  /**
   * Creates a bloom over the bloomOver expression, with an expected number of rows and fpp to build.
   *
   * This bloom is an array of byte arrays, with the underlying arrays stored on file, as such it is only limited by file system size.
   *
   * @param bloomOver
   * @param expectedNumberOfRows
   * @param expectedFPP
   * @return
   */
  def big_bloom(bloomOver: Column, expectedNumberOfRows: Long, expectedFPP: Double): Column =
    big_bloom(bloomOver, lit(expectedNumberOfRows), lit(expectedFPP))

  /**
   * Creates a bloom over the bloomOver expression, with an expected number of rows and fpp to build.
   *
   * This bloom is an array of byte arrays, with the underlying arrays stored on file, as such it is only limited by file system size.
   *
   * @param bloomOver
   * @param expectedNumberOfRows
   * @param expectedFPP
   * @param id                - the id of this bloom used to separate from other big_blooms - defaults to a uuid
   * @return
   */
  def big_bloom(bloomOver: Column, expectedNumberOfRows: Long, expectedFPP: Double, id: String): Column =
    big_bloom(bloomOver, lit(expectedNumberOfRows), lit(expectedFPP), lit(id))

  /**
   * Creates a bloom over the bloomOver expression, with an expected number of rows and fpp to build.
   *
   * This bloom is an array of byte arrays, with the underlying arrays stored on file, as such it is only limited by file system size.
   *
   * @param bloomOver
   * @param expectedNumberOfRows
   * @param expectedFPP
   * @param id                - the id of this bloom used to separate from other big_blooms - defaults to a uuid
   * @param bucketedFilesRoot - provide this to override the default file root for the underlying arrays - defaults to a temporary file location or /dbfs/ on Databricks.  The config property sparkutils.quality.bloom.root can also be used to set for all blooms in a cluster.
   * @return
   */
  def big_bloom(bloomOver: Column, expectedNumberOfRows: Long, expectedFPP: Double, id: String, bucketedFilesRoot: BucketedFilesRoot): Column =
    big_bloom(bloomOver, lit(expectedNumberOfRows), lit(expectedFPP), lit(id), bucketedFilesRoot)

}