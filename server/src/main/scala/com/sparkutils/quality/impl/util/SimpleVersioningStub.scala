package com.sparkutils.quality.impl.util

import com.sparkutils.testing.ConnectWhenForced.someOrForcedConnect
import org.apache.spark.sql.{Column, DataFrame}

/**
 * A simple versioning scheme that allows management of versions
 */
protected[quality] object SimpleVersioningStub {

  /**
   * Reads the rules table and builds complete rule versions by adding together all changes below that rulesuiteVersion
   *
   * @param df
   * @param ruleSuiteId
   * @param ruleSuiteVersion
   * @param ruleSetId
   * @param ruleSetVersion
   * @param ruleId
   * @param ruleVersion
   * @param ruleExpr
   * @param ruleEngineSalience
   * @param ruleEngineId
   * @param ruleEngineVersion
   * @return
   */
  protected[quality] def readVersionedRuleRowsFromDF(df: DataFrame,
                                  ruleSuiteId: Column,
                                  ruleSuiteVersion: Column,
                                  ruleSetId: Column,
                                  ruleSetVersion: Column,
                                  ruleId: Column,
                                  ruleVersion: Column,
                                  ruleExpr: Column,
                                  ruleEngineSalience: Column,
                                  ruleEngineId: Column,
                                  ruleEngineVersion: Column
                                 ): Option[DataFrame] = someOrForcedConnect(
    SimpleVersioning.readVersionedRuleRowsFromDF(df, ruleSuiteId, ruleSuiteVersion, ruleSetId, ruleSetVersion, ruleId,
      ruleVersion, ruleExpr, ruleEngineSalience, ruleEngineId, ruleEngineVersion)
  )

  /**
   * Reads the lambda table and builds lambda versions by adding together all changes below that rulesuiteVersion
   * @param lambdaFunctionDF
   * @param lambdaFunctionName
   * @param lambdaFunctionExpression
   * @param lambdaFunctionId
   * @param lambdaFunctionVersion
   * @param lambdaFunctionRuleSuiteId
   * @param lambdaFunctionRuleSuiteVersion
   * @return
   */
  protected[quality] def readVersionedLambdaRowsFromDF(lambdaFunctionDF: DataFrame,
                                lambdaFunctionName: Column,
                                lambdaFunctionExpression: Column,
                                lambdaFunctionId: Column,
                                lambdaFunctionVersion: Column,
                                lambdaFunctionRuleSuiteId: Column,
                                lambdaFunctionRuleSuiteVersion: Column
                              ): Option[DataFrame] = someOrForcedConnect(
    SimpleVersioning.readVersionedLambdaRowsFromDF(lambdaFunctionDF, lambdaFunctionName, lambdaFunctionExpression,
      lambdaFunctionId, lambdaFunctionVersion, lambdaFunctionRuleSuiteId, lambdaFunctionRuleSuiteVersion)
  )

  /**
   * Reads the output expression table and builds output expression versions by adding together all changes below that rulesuiteVersion
   *
   * @param outputExpressionDF
   * @param outputExpression
   * @param outputExpressionId
   * @param outputExpressionVersion
   * @param outputExpressionRuleSuiteId
   * @param outputExpressionRuleSuiteVersion
   * @return
   */
  protected[quality] def readVersionedOutputExpressionRowsFromDF(outputExpressionDF: DataFrame,
                                              outputExpression: Column,
                                              outputExpressionId: Column,
                                              outputExpressionVersion: Column,
                                              outputExpressionRuleSuiteId: Column,
                                              outputExpressionRuleSuiteVersion: Column
                                             ): Option[DataFrame] = someOrForcedConnect(
    SimpleVersioning.readVersionedOutputExpressionRowsFromDF(outputExpressionDF, outputExpression, outputExpressionId,
      outputExpressionVersion, outputExpressionRuleSuiteId, outputExpressionRuleSuiteVersion)
  )

}
