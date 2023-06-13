package com.sparkutils.quality.impl.bloom

import com.sparkutils.quality.RuleSuite
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Column, QualitySparkUtils, SparkSession}
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
}
