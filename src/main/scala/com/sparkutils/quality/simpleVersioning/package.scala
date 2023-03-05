package com.sparkutils.quality

import com.sparkutils.quality.RuleModel.RuleSuiteMap
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.col

import scala.collection.immutable.TreeMap

// Used to pull in |+| to deep merge the maps as SemiGroups - https://typelevel.org/cats/typeclasses/semigroup.html#example-usage-merging-maps
import cats.implicits._

/**
 * A simple versioning scheme that allows management of versions
 */
package object simpleVersioning {

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
                             ): RuleSuiteMap = {
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
      ruleEngineVersion.as("ruleEngineVersion").cast(IntegerType)
    ).createOrReplaceTempView("rules")

    val versionedRules = df.sparkSession.sql(
      """
       select ruleExpr, ruleId, ruleVersion, ruleEngineSalience, ruleEngineId, ruleEngineVersion,
       ruleSetId, ruleSetVersion, -- don't need to bump versions as they can coexist
        suiteversions.ruleSuiteId, suiteversions.ruleSuiteVersion -- force the versions to be bumped to latest ruleSuite Versions
        from
         (select distinct ruleSuiteId, ruleSuiteVersion from rules) suiteversions join
         rules l0 on l0.ruleSuiteId = suiteversions.ruleSuiteId and l0.ruleSuiteVersion <= suiteversions.ruleSuiteVersion
         where
          not exists (
            select 0 from rules l1
            where l1.ruleSuiteId = l0.ruleSuiteId
             and l1.ruleId = l0.ruleId
             and l1.ruleSetId = l0.ruleSetId
             and l1.ruleSuiteVersion <= suiteversions.ruleSuiteVersion
             and l1.ruleVersion > l0.ruleVersion
          )
          and l0.ruleExpr != "DELETED"
       """)

    readRulesFromDF(versionedRules,
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
                              ): Map[Id, Seq[LambdaFunction]] = {
    lambdaFunctionDF.select(
      lambdaFunctionName.as("name"),
      lambdaFunctionExpression.as("ruleExpr"),
      lambdaFunctionId.as("functionId").cast(IntegerType),
      lambdaFunctionVersion.as("functionVersion").cast(IntegerType),
      lambdaFunctionRuleSuiteId.as("ruleSuiteId").cast(IntegerType),
      lambdaFunctionRuleSuiteVersion.as("ruleSuiteVersion").cast(IntegerType)
    ).createOrReplaceTempView("lambdas")

    val versionedLambdas = lambdaFunctionDF.sparkSession.sql(
      lambdaOutputSQL("lambdas", "name, "))

    readLambdasFromDF(versionedLambdas,
      col("name"),
      col("ruleExpr"),
      col("functionId"),
      col("functionVersion"),
      col("ruleSuiteId"),
      col("ruleSuiteVersion")
    )
  }

  protected[quality] def lambdaOutputSQL(tableName: String, extra: String ="") =
      s"""
       select $extra ruleExpr, functionId, functionVersion,
        versions.ruleSuiteId, versions.ruleSuiteVersion -- force the versions to be bumped to latest ruleSuiteVersions
        from
         (select distinct ruleSuiteId, ruleSuiteVersion from $tableName) versions join
         $tableName l0 on l0.ruleSuiteId = versions.ruleSuiteId and l0.ruleSuiteVersion <= versions.ruleSuiteVersion
         where
          not exists (
            select 0 from $tableName l1
            where l1.ruleSuiteId = l0.ruleSuiteId and
             l1.functionId = l0.functionId and
             l1.ruleSuiteVersion <= versions.ruleSuiteVersion
             and l1.functionVersion > l0.functionVersion
          )
          and l0.ruleExpr != "DELETED"
       """

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
              ): Map[Id, Seq[OutputExpressionRow]] = {
    outputExpressionDF.select(
      outputExpression.as("ruleExpr"),
      outputExpressionId.as("functionId").cast(IntegerType),
      outputExpressionVersion.as("functionVersion").cast(IntegerType),
      outputExpressionRuleSuiteId.as("ruleSuiteId").cast(IntegerType),
      outputExpressionRuleSuiteVersion.as("ruleSuiteVersion").cast(IntegerType)
    ).createOrReplaceTempView("outputExpressions")

    val versionedOutputs = outputExpressionDF.sparkSession.sql(
      lambdaOutputSQL("outputExpressions"))

    readOutputExpressionsFromDF(versionedOutputs,
      col("ruleExpr"),
      col("functionId"),
      col("functionVersion"),
      col("ruleSuiteId"),
      col("ruleSuiteVersion"))
  }

  protected[quality] case class SameOrNextVersionLower[T](map: Map[Id, Seq[T]]) extends Function1[Id, Option[Seq[T]]] {
    implicit val ordering = new Ordering[Id] {
      def compare(x: Id, y: Id): Int = {
        if (x == y)
          0
        else
          if (x.id != y.id)
            x.id - y.id
          else
            x.version - y.version
      }
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
    iIntegrateLambdas(ruleSuiteMap, lambdas, globalLibrary, SameOrNextVersionLower(lambdas))

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
    iIntegrateOutputExpressions(ruleSuiteMap, outputs, globalLibrary, SameOrNextVersionLower(outputs))

}
