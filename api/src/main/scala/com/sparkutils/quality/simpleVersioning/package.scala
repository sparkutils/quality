package com.sparkutils.quality

import com.sparkutils.quality.RuleModel.RuleSuiteMap
import com.sparkutils.quality.impl.util.{GeneratedUniqueName, Serializing, SimpleVersioningStub}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.col
import com.sparkutils.quality.impl.extension.QualityVersionedRulesConstants.{QUALITY_VERSIONED_LAMBDAS_FROM_DF, QUALITY_VERSIONED_OUTPUT_EXPRESSIONS_FROM_DF, QUALITY_VERSIONED_RULES_FROM_DF}

import scala.collection.immutable.TreeMap

// Used to pull in |+| to deep merge the maps as SemiGroups - https://typelevel.org/cats/typeclasses/semigroup.html#example-usage-merging-maps
import cats.implicits._

/**
 * A simple versioning scheme that allows management of versions
 */
package object simpleVersioning extends GeneratedUniqueName {

  protected val GENERATED_NAME_PREFIX = "QUALITY_VERSIONING_GENERATED_NAME_"

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
  def readVersionedRuleRowsFromDF(df: DataFrame,
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
                                 ): DataFrame =
    SimpleVersioningStub.readVersionedRuleRowsFromDF(df, ruleSuiteId, ruleSuiteVersion, ruleSetId, ruleSetVersion,
      ruleId, ruleVersion, ruleExpr, ruleEngineSalience, ruleEngineId, ruleEngineVersion).getOrElse{
      val name = uniqueName()
      df.select(
        ruleSuiteId.as("ruleSuiteId").cast(IntegerType),
        ruleSuiteVersion.as("ruleSuiteVersion").cast(IntegerType),
        ruleSetId.as("ruleSetId").cast(IntegerType),
        ruleSetVersion.as("ruleSetVersion").cast(IntegerType),
        ruleId.as("ruleId").cast(IntegerType),
        ruleVersion.as("ruleVersion").cast(IntegerType),
        ruleExpr.as("ruleExpr"),
        ruleEngineSalience.as("ruleEngineSalience").cast(IntegerType),
        ruleEngineId.as("ruleEngineId").cast(IntegerType),
        ruleEngineVersion.as("ruleEngineVersion").cast(IntegerType)).createOrReplaceTempView(name)

      df.sparkSession.sql(QUALITY_VERSIONED_RULES_FROM_DF + name)
    }

  /**
   * Reads the rules table and builds complete rule versions by adding together all changes below that rulesuiteVersion
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
  def readVersionedRulesFromDF(df: DataFrame,
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
    readRulesFromDF(readVersionedRuleRowsFromDF(df: DataFrame,
      ruleSuiteId: Column,
      ruleSuiteVersion: Column,
      ruleSetId: Column,
      ruleSetVersion: Column,
      ruleId: Column,
      ruleVersion: Column,
      ruleExpr: Column,
      ruleEngineSalience: Column,
      ruleEngineId: Column,
      ruleEngineVersion: Column),
      col("ruleSuiteId"),
      col("ruleSuiteVersion"),
      col("ruleSetId"),
      col("ruleSetVersion"),
      col("ruleId"),
      col("ruleVersion"),
      col("ruleExpr"),
      col("ruleEngineSalience"),
      col("ruleEngineId"),
      col("ruleEngineVersion")
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
  def readVersionedLambdaRowsFromDF(lambdaFunctionDF: DataFrame,
                                lambdaFunctionName: Column,
                                lambdaFunctionExpression: Column,
                                lambdaFunctionId: Column,
                                lambdaFunctionVersion: Column,
                                lambdaFunctionRuleSuiteId: Column,
                                lambdaFunctionRuleSuiteVersion: Column
                              ): DataFrame =
    SimpleVersioningStub.readVersionedLambdaRowsFromDF(lambdaFunctionDF, lambdaFunctionName, lambdaFunctionExpression,
      lambdaFunctionId, lambdaFunctionVersion, lambdaFunctionRuleSuiteId, lambdaFunctionRuleSuiteVersion).getOrElse{
      val name = uniqueName()
      lambdaFunctionDF.select(
        lambdaFunctionName.as("name"),
        lambdaFunctionExpression.as("ruleExpr"),
        lambdaFunctionId.as("functionId").cast(IntegerType),
        lambdaFunctionVersion.as("functionVersion").cast(IntegerType),
        lambdaFunctionRuleSuiteId.as("ruleSuiteId").cast(IntegerType),
        lambdaFunctionRuleSuiteVersion.as("ruleSuiteVersion").cast(IntegerType)
      ).createOrReplaceTempView(name)

      lambdaFunctionDF.sparkSession.sql(QUALITY_VERSIONED_LAMBDAS_FROM_DF + name)
    }

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
  def readVersionedLambdasFromDF(lambdaFunctionDF: DataFrame,
                                 lambdaFunctionName: Column,
                                 lambdaFunctionExpression: Column,
                                 lambdaFunctionId: Column,
                                 lambdaFunctionVersion: Column,
                                 lambdaFunctionRuleSuiteId: Column,
                                 lambdaFunctionRuleSuiteVersion: Column
                                ): Map[Id, Seq[LambdaFunction]] =
    readLambdasFromDF(readVersionedLambdaRowsFromDF(lambdaFunctionDF: DataFrame,
      lambdaFunctionName: Column,
      lambdaFunctionExpression: Column,
      lambdaFunctionId: Column,
      lambdaFunctionVersion: Column,
      lambdaFunctionRuleSuiteId: Column,
      lambdaFunctionRuleSuiteVersion: Column),
      col("name"),
      col("ruleExpr"),
      col("functionId"),
      col("functionVersion"),
      col("ruleSuiteId"),
      col("ruleSuiteVersion")
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
  def readVersionedOutputExpressionRowsFromDF(outputExpressionDF: DataFrame,
                                              outputExpression: Column,
                                              outputExpressionId: Column,
                                              outputExpressionVersion: Column,
                                              outputExpressionRuleSuiteId: Column,
                                              outputExpressionRuleSuiteVersion: Column
                                             ): DataFrame =
    SimpleVersioningStub.readVersionedOutputExpressionRowsFromDF(outputExpressionDF, outputExpression,
      outputExpressionId, outputExpressionVersion, outputExpressionRuleSuiteId, outputExpressionRuleSuiteVersion).getOrElse{
      val name = uniqueName()
      outputExpressionDF.select(
        outputExpression.as("ruleExpr"),
        outputExpressionId.as("functionId").cast(IntegerType),
        outputExpressionVersion.as("functionVersion").cast(IntegerType),
        outputExpressionRuleSuiteId.as("ruleSuiteId").cast(IntegerType),
        outputExpressionRuleSuiteVersion.as("ruleSuiteVersion").cast(IntegerType)
      ).createOrReplaceTempView(name)

      outputExpressionDF.sparkSession.sql(QUALITY_VERSIONED_OUTPUT_EXPRESSIONS_FROM_DF + name)
    }

  /**
   * Reads the output expression table and builds output expression versions by adding together all changes below that rulesuiteVersion
   * @param outputExpressionDF
   * @param outputExpression
   * @param outputExpressionId
   * @param outputExpressionVersion
   * @param outputExpressionRuleSuiteId
   * @param outputExpressionRuleSuiteVersion
   * @return
   */
  def readVersionedOutputExpressionsFromDF(outputExpressionDF: DataFrame,
                                           outputExpression: Column,
                                           outputExpressionId: Column,
                                           outputExpressionVersion: Column,
                                           outputExpressionRuleSuiteId: Column,
                                           outputExpressionRuleSuiteVersion: Column
                                          ): Map[Id, Seq[OutputExpressionRow]] =
    readOutputExpressionsFromDF(readVersionedOutputExpressionRowsFromDF(outputExpressionDF: DataFrame,
      outputExpression: Column,
      outputExpressionId: Column,
      outputExpressionVersion: Column,
      outputExpressionRuleSuiteId: Column,
      outputExpressionRuleSuiteVersion: Column
      ),
      col("ruleExpr"),
      col("functionId"),
      col("functionVersion"),
      col("ruleSuiteId"),
      col("ruleSuiteVersion"))

  protected[quality] case class SameOrNextVersionLower[T](map: Map[Id, Seq[T]]) extends Function1[Id, Option[Seq[T]]] {
    implicit val ordering: Ordering[Id] =
      (x: Id, y: Id) => {
        if (x == y)
          0
        else
          if (x.id != y.id)
            x.id - y.id
          else
            x.version - y.version
      }

    val sorted = TreeMap[Id, Seq[T]]() ++ map

    override def apply(id: Id): Option[Seq[T]] =
      sorted.to(id).lastOption.map(_._2)
  }

  /**
   * Add any of the Lambdas loaded for the rule suites
   *
   * @param ruleSuiteMap
   * @param lambdas
   * @param globalLibrary - all lambdas with this RuleSuite Id will be added to all RuleSuites
   * @return
   */
  def integrateVersionedLambdas(ruleSuiteMap: RuleSuiteMap, lambdas: Map[Id, Seq[LambdaFunction]], globalLibrary: Option[Id] = None): RuleSuiteMap =
    Serializing.iIntegrateLambdas(ruleSuiteMap, lambdas, globalLibrary, SameOrNextVersionLower(lambdas))

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
  def integrateVersionedOutputExpressions(ruleSuiteMap: RuleSuiteMap, outputs: Map[Id, Seq[OutputExpressionRow]], globalLibrary: Option[Id] = None): (RuleSuiteMap, Map[Id, Set[Rule]]) =
    Serializing.iIntegrateOutputExpressions(ruleSuiteMap, outputs, globalLibrary, SameOrNextVersionLower(outputs))

}
