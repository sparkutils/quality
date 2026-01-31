package com.sparkutils.quality.impl.util

import com.sparkutils.quality.impl.util.RuleModel.RuleSuiteMap
import com.sparkutils.quality._
import com.sparkutils.quality.LambdaFunction
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
  def readLambdaRowsFromDF(
                            lambdaFunctionDF: DataFrame,
                            lambdaFunctionName: Column,
                            lambdaFunctionExpression: Column,
                            lambdaFunctionId: Column,
                            lambdaFunctionVersion: Column,
                            lambdaFunctionRuleSuiteId: Column,
                            lambdaFunctionRuleSuiteVersion: Column
                          ): Dataset[LambdaFunctionRow] =
    Serializing.readLambdaRowsFromDF(
      lambdaFunctionDF,
      lambdaFunctionName,
      lambdaFunctionExpression,
      lambdaFunctionId,
      lambdaFunctionVersion,
      lambdaFunctionRuleSuiteId,
      lambdaFunctionRuleSuiteVersion
    )

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
  def readOutputExpressionRowsFromDF(
                                      outputExpressionDF: DataFrame,
                                      outputExpression: Column,
                                      outputExpressionId: Column,
                                      outputExpressionVersion: Column,
                                      outputExpressionRuleSuiteId: Column,
                                      outputExpressionRuleSuiteVersion: Column
                                    ): Dataset[OutputExpressionRow] =
    Serializing.readOutputExpressionRowsFromDF(
      outputExpressionDF,
      outputExpression,
      outputExpressionId,
      outputExpressionVersion,
      outputExpressionRuleSuiteId,
      outputExpressionRuleSuiteVersion
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
   * Loads RuleRows from a dataframe with integers ruleSuiteId, ruleSuiteVersion, ruleSetId, ruleSetVersion, ruleId, ruleVersion and an expression string ruleExpr
   */
  def readRuleRowsFromDF(df: DataFrame,
                         ruleSuiteId: Column,
                         ruleSuiteVersion: Column,
                         ruleSetId: Column,
                         ruleSetVersion: Column,
                         ruleId: Column,
                         ruleVersion: Column,
                         ruleExpr: Column,
                         ruleEngine: Option[(Column, Column, Column)] = None
                        ): Dataset[RuleRow] =
    Serializing.readRuleRowsFromDF(
      df,
      ruleSuiteId,
      ruleSuiteVersion,
      ruleSetId,
      ruleSetVersion,
      ruleId,
      ruleVersion,
      ruleExpr,
      ruleEngine
    )

  /**
   * Loads RuleRows from a dataframe with integers ruleSuiteId, ruleSuiteVersion, ruleSetId, ruleSetVersion, ruleId, ruleVersion and an expression string ruleExpr
   */
  def readRuleRowsFromDF(df: DataFrame,
                         ruleSuiteId: Column,
                         ruleSuiteVersion: Column,
                         ruleSetId: Column,
                         ruleSetVersion: Column,
                         ruleId: Column,
                         ruleVersion: Column,
                         ruleExpr: Column,
                         outputExpressionSalience: Column,
                         outputExpressionId: Column,
                         outputExpressionVersion: Column
                        ): Dataset[RuleRow] =
    Serializing.readRuleRowsFromDF(
      df,
      ruleSuiteId,
      ruleSuiteVersion,
      ruleSetId,
      ruleSetVersion,
      ruleId,
      ruleVersion,
      ruleExpr,
      Some((outputExpressionSalience, outputExpressionId, outputExpressionVersion))
    )

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
   * Returns an integrated ruleSuiteMap with probablePass and defaultOutputExpression applied
   *
   * Call this before calling integrateOutputExpressions.
   *
   * @param ruleSuiteMap
   * @param outputs
   * @param globalLibrary
   * @return
   */
  def integrateRuleSuites(ruleSuiteMap: RuleSuiteMap, ruleSuites: Map[Id, RuleSuiteRow]): RuleSuiteMap =
    Serializing.integrateRuleSuites(ruleSuiteMap, ruleSuites)

  /**
   * Loads RuleSuite specific attributes to use with integrateRuleSuites
   * @param ruleSuites
   * @return
   */
  def readRuleSuitesFromDF(ruleSuites: Dataset[RuleSuiteRow]): Map[Id, RuleSuiteRow] =
    ruleSuites.collect().map(r => Id(r.ruleSuiteId, r.ruleSuiteVersion) -> r).toMap

  /**
   * Identify if the missing OutputExpression from a RuleSuite is from a defaultProcessor
   * @param rule
   * @return
   */
  def isAMissingRuleSuiteRule(rule: Rule): Boolean = Serializing.isAMissingRuleSuiteRule(rule)

  /**
   * Returns an integrated ruleSuiteMap with a set of RuleSuite Id -> Rule mappings where the OutputExpression didn't exist.
   * If defaultProcessor is expected to be used then call integrateRuleSuites *before*.
   *
   * Users should check if their RuleSuite is in the "error" map.  The isAMissingRuleSuiteRule can be used to identify if a Rule is referring to a missing RuleSuite.defaultProcessor
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
   * Creates a RuleSuiteRow from a RuleSuite for RuleSuite specific attributes and an optional defaultProcessor
   * @param ruleSuite
   */
  def toRuleSuiteRow(ruleSuite: RuleSuite): (RuleSuiteRow, Option[OutputExpressionRow]) =
    (RuleSuiteRow(ruleSuite.id.id, ruleSuite.id.version, ruleSuite.probablePass,
      ruleSuite.defaultProcessor.id.id, ruleSuite.defaultProcessor.id.version),
      if (ruleSuite.defaultProcessor eq NoOpRunOnPassProcessor.noOp)
        None
      else
        Some(OutputExpressionRow(ruleSuite.defaultProcessor.rule, ruleSuite.defaultProcessor.id.id,
          ruleSuite.defaultProcessor.id.version, ruleSuite.id.id, ruleSuite.id.version))
      )

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
