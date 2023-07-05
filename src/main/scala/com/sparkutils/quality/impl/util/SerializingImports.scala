package com.sparkutils.quality.impl.util

import com.sparkutils.quality.impl.util.RuleModel.RuleSuiteMap
import com.sparkutils.quality._
import com.sparkutils.quality.impl.LambdaFunction
import com.sparkutils.quality.impl.util.Serializing.{iIntegrateLambdas, iIntegrateOutputExpressions, ireadRulesFromDF}
import org.apache.spark.sql.{Column, DataFrame, Dataset}

trait SerializingImports {


  /**
   * Loads lambda functions
   * @param lambdaFunctionDF
   * @param lambdaFunctionRuleSuiteId
   * @param lambdaFunctionRuleSuiteVersion
   * @param lambdaFunctionId
   * @param lambdaFunctionVersion
   * @param lambdaFunctionName
   * @param lambdaFunctionExpression
   * @return
   */
  def readLambdasFromDF(
                         lambdaFunctionDF: DataFrame,
                         lambdaFunctionName: Column,
                         lambdaFunctionExpression: Column,
                         lambdaFunctionId: Column,
                         lambdaFunctionVersion: Column,
                         lambdaFunctionRuleSuiteId: Column,
                         lambdaFunctionRuleSuiteVersion: Column
                       ): Map[Id, Seq[LambdaFunction]] =
    Serializing.readLambdasFromDF(
      lambdaFunctionDF,
      lambdaFunctionName,
      lambdaFunctionExpression,
      lambdaFunctionId,
      lambdaFunctionVersion,
      lambdaFunctionRuleSuiteId,
      lambdaFunctionRuleSuiteVersion
    )

  /**
   * Loads output expressions
   * @param outputExpressionDF
   * @param outputExpressionRuleSuiteId
   * @param outputExpressionRuleSuiteVersion
   * @param outputExpressionId
   * @param outputExpressionVersion
   * @param outputExpression
   * @return
   */
  def readOutputExpressionsFromDF(
                                   outputExpressionDF: DataFrame,
                                   outputExpression: Column,
                                   outputExpressionId: Column,
                                   outputExpressionVersion: Column,
                                   outputExpressionRuleSuiteId: Column,
                                   outputExpressionRuleSuiteVersion: Column
                                 ): Map[Id, Seq[OutputExpressionRow]] =
    Serializing.readOutputExpressionsFromDF(
      outputExpressionDF,
      outputExpression,
      outputExpressionId,
      outputExpressionVersion,
      outputExpressionRuleSuiteId,
      outputExpressionRuleSuiteVersion
    )

  def readMetaRuleSetsFromDF(
                              metaRuleSetDF: DataFrame,
                              columnSelectionExpression: Column,
                              lambdaFunctionExpression: Column,
                              metaRuleSetId: Column,
                              metaRuleSetVersion: Column,
                              metaRuleSuiteId: Column,
                              metaRuleSuiteVersion: Column
                            ): Map[Id, Seq[MetaRuleSetRow]] =
    Serializing.readMetaRuleSetsFromDF(metaRuleSetDF,
      columnSelectionExpression,
      lambdaFunctionExpression,
      metaRuleSetId,
      metaRuleSetVersion,
      metaRuleSuiteId,
      metaRuleSuiteVersion)

  /**
   * Loads a RuleSuite from a dataframe with integers ruleSuiteId, ruleSuiteVersion, ruleSetId, ruleSetVersion, ruleId, ruleVersion and an expression string ruleExpr
   */
  def readRulesFromDF(df: DataFrame,
                      ruleSuiteId: Column,
                      ruleSuiteVersion: Column,
                      ruleSetId: Column,
                      ruleSetVersion: Column,
                      ruleId: Column,
                      ruleVersion: Column,
                      ruleExpr: Column
                     ): RuleSuiteMap =
    ireadRulesFromDF(df,
      ruleSuiteId,
      ruleSuiteVersion,
      ruleSetId,
      ruleSetVersion,
      ruleId,
      ruleVersion,
      ruleExpr)

  /**
   * Loads a RuleSuite from a dataframe with integers ruleSuiteId, ruleSuiteVersion, ruleSetId, ruleSetVersion, ruleId, ruleVersion and an expression string ruleExpr
   */
  def readRulesFromDF(df: DataFrame,
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
                     ): RuleSuiteMap =
    ireadRulesFromDF(df,
      ruleSuiteId,
      ruleSuiteVersion,
      ruleSetId,
      ruleSetVersion,
      ruleId,
      ruleVersion,
      ruleExpr,
      Some((ruleEngineSalience, ruleEngineId, ruleEngineVersion)))

  /**
   * Add any of the Lambdas loaded for the rule suites
   *
   * @param ruleSuiteMap
   * @param lambdas
   * @param globalLibrary - all lambdas with this RuleSuite Id will be added to all RuleSuites
   * @return
   */
  def integrateLambdas(ruleSuiteMap: RuleSuiteMap, lambdas: Map[Id, Seq[LambdaFunction]], globalLibrary: Option[Id] = None): RuleSuiteMap =
    iIntegrateLambdas(ruleSuiteMap, lambdas, globalLibrary, r => lambdas.get(r))


  /**
   * Returns an integrated ruleSuiteMap with a set of RuleSuite Id -> Rule mappings where the OutputExpression didn't exist.
   *
   * Users should check if their RuleSuite is in the "error" map.
   *
   * @param ruleSuiteMap
   * @param outputs
   * @param globalLibrary
   * @return
   */
  def integrateOutputExpressions(ruleSuiteMap: RuleSuiteMap, outputs: Map[Id, Seq[OutputExpressionRow]], globalLibrary: Option[Id] = None): (RuleSuiteMap, Map[Id, Set[Rule]]) =
    iIntegrateOutputExpressions(ruleSuiteMap, outputs, globalLibrary, id => outputs.get(id))

  /**
   * Integrates meta rulesets into the rulesuites.  Note this only works for a specific dataset, if rulesuites should be
   * filtered for a given dataset then this must take place before calling.
   *
   * @param dataFrame the dataframe to identify columns for metarules
   * @param ruleSuiteMap the ruleSuites relevant for this dataframe
   * @param metaRuleSetMap the meta rulesets
   * @param stablePosition this function must maintain the law that each column name within a RuleSet always generates the same position
   * @return
   */
  def integrateMetaRuleSets(dataFrame: DataFrame, ruleSuiteMap: RuleSuiteMap, metaRuleSetMap: Map[Id, Seq[MetaRuleSetRow]], stablePosition: String => Int, transform: DataFrame => DataFrame = identity): RuleSuiteMap =
    Serializing.iIntegrateMetaRuleSets(dataFrame, ruleSuiteMap, metaRuleSetMap, stablePosition, transform)

  /**
   * Utility function to easy dealing with simple DQ rules where the rule engine functionality is ignored.
   * This can be paired with the default ruleEngine parameter in readRulesFromDF
   */
  def toRuleSuiteDF(ruleSuite: RuleSuite): DataFrame =
    toDS(ruleSuite).drop("ruleEngineSalience", "ruleEngineExpr")

  /**
   * Must have an active sparksession before calling and only works with ExpressionRule's, all other rules are converted to 1=1
   * @param ruleSuite
   * @return a Dataset[RowRaw] with the rules flattened out
   */
  def toDS(ruleSuite: RuleSuite): Dataset[RuleRow] =
    Serializing.toDS(ruleSuite)

  /**
   * Must have an active sparksession before calling and only works with ExpressionRule's, all other rules are converted to 1=1
   * @param ruleSuite
   * @return a Dataset[RowRaw] with the rules flattened out
   */
  def toLambdaDS(ruleSuite: RuleSuite): Dataset[LambdaFunctionRow] =
    Serializing.toLambdaDS(ruleSuite)

  /**
   * Must have an active sparksession before calling and only works with ExpressionRule's, all other rules are converted to 1=1
   * @param ruleSuite
   * @return a Dataset[RowRaw] with the rules flattened out
   */
  def toOutputExpressionDS(ruleSuite: RuleSuite): Dataset[OutputExpressionRow] =
    Serializing.toOutputExpressionDS(ruleSuite)

}
