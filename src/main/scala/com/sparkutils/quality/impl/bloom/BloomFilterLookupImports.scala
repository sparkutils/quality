package com.sparkutils.quality.impl.bloom

import com.sparkutils.quality.{DataFrameLoader, Id, RuleSuite}
import com.sparkutils.quality.impl.util.ConfigLoader
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Column, DataFrame, QualitySparkUtils, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression

trait BloomFilterRegistration {

  /**
   * Registers this bloom map and associates the probabilityIn sql expression against it
   * @param bloomFilterMap
   */
  def registerBloomMapAndFunction(bloomFilterMap: Broadcast[BloomExpressionLookup.BloomFilterMap]) {
    val funcReg = SparkSession.getActiveSession.get.sessionState.functionRegistry

    val f = (exps: Seq[Expression]) => BloomFilterLookupExpression(exps(0), exps(1), bloomFilterMap)
    QualitySparkUtils.registerFunction(funcReg)("probabilityIn", f)
  }

}


trait BloomFilterLookupImports {

  /**
   * Identifies bloom ids before (or after) resolving for a given ruleSuite, use to know which bloom filters need to be loaded
   *
   * @param ruleSuite a ruleSuite full of expressions to check
   * @return The bloom id's used, for unresolved expression trees this may contain blooms which are not present in the bloom map
   */
  def getBlooms(ruleSuite: RuleSuite): Seq[String] = BloomFilterLookup.getBlooms(ruleSuite)

  def bloomFilterLookup(bloomFilterName: Column, lookupValue: Column, bloomMap: Broadcast[BloomExpressionLookup.BloomFilterMap]): Column =
    BloomFilterLookup(bloomFilterName, lookupValue, bloomMap)


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
