package com.sparkutils.quality.impl.util

import com.sparkutils.quality.impl.util.RuleModel.RuleSuiteMap
import com.sparkutils.quality.impl.{LambdaFunction, NoOpRunOnPassProcessor, RuleRunnerUtils, RunOnPassProcessor, RunOnPassProcessorHolder, VersionedId}
import com.sparkutils.quality.impl.imports.RuleResultsImports.packId
import com.sparkutils.quality._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.expr

import scala.collection.JavaConverters._
import scala.collection.mutable

// Used to pull in |+| to deep merge the maps as SemiGroups - https://typelevel.org/cats/typeclasses/semigroup.html#example-usage-merging-maps
import cats.implicits._

object Serializing {

  def ruleResultToInt(ruleResult: RuleResult): Int = RuleRunnerUtils.ruleResultToInt(ruleResult)

  def flatten(ruleSuiteResult: RuleSuiteResult): Iterable[RuleResultRow] =
    ruleSuiteResult.ruleSetResults.flatMap{
      case (rsId, rsR) =>
        rsR.ruleResults.map {
          case (rId, rRes) =>

            RuleResultRow(ruleSuiteResult.id.id,
              ruleSuiteResult.id.version,
              ruleResultToInt(ruleSuiteResult.overallResult),
              ruleResultToInt(rsR.overallResult),
              rsId.id,
              rsId.version,
              rId.id,
              rId.version,
              ruleResultToInt(rRes)
            )
        }
    }

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
  protected[quality] def iIntegrateMetaRuleSets(dataFrame: DataFrame, ruleSuiteMap: RuleSuiteMap, metaRuleSetMap: Map[Id, Seq[MetaRuleSetRow]], stablePosition: String => Int, transform: DataFrame => DataFrame = identity): RuleSuiteMap =
    ruleSuiteMap.mapValues(r => metaRuleSetMap.get(r.id).map{
      metaRuleSets =>
        val generatedRuleSets = metaRuleSets.map( mrs => mrs.generateRuleSet(dataFrame, stablePosition, transform))
        r.copy(ruleSets = r.ruleSets ++ generatedRuleSets)
    }.getOrElse(r))

  protected[quality] def iIntegrateLambdas(ruleSuiteMap: RuleSuiteMap, lambdas: Map[Id, Seq[LambdaFunction]], globalLibrary: Option[Id], get: Id => Option[Seq[LambdaFunction]]): RuleSuiteMap = {
    val shared = globalLibrary.map(lambdas.getOrElse(_, Set()).toSet).getOrElse(Set())
    ruleSuiteMap.mapValues(r => get(r.id).map(lambdas => r.copy(lambdaFunctions = (lambdas.toSet |+| shared).toSeq)).getOrElse(r.copy(lambdaFunctions = shared.toSeq)))
  }

  protected[quality] def iIntegrateOutputExpressions(ruleSuiteMap: RuleSuiteMap, outputs: Map[Id, Seq[OutputExpressionRow]], globalLibrary: Option[Id], get: Id => Option[Seq[OutputExpressionRow]]): (RuleSuiteMap, Map[Id, Set[Rule]]) = {
    val shared = globalLibrary.map(outputs.getOrElse(_, Seq.empty)).getOrElse(Seq.empty)
    val notexists = mutable.Map.empty[Id, Set[Rule]]

    val alreadyProcessed = mutable.Map.empty[Id, RunOnPassProcessor]

    val map =
      ruleSuiteMap.mapValues { rsuite =>

        val outputSeq = get(rsuite.id).map(os => os).getOrElse(Seq.empty) ++ shared
        val map = outputSeq.map(oe => Id(oe.functionId, oe.functionVersion) -> oe).toMap

        rsuite.copy(ruleSets = rsuite.ruleSets.map {
          rs =>
            rs.copy(rules = rs.rules.map {
              r =>
                if (r.runOnPassProcessor ne NoOpRunOnPassProcessor.noOp)
                  map.get(r.runOnPassProcessor.id).map { oe =>
                    r.copy(runOnPassProcessor =
                      alreadyProcessed.getOrElse(r.runOnPassProcessor.id, {
                        val processed = r.runOnPassProcessor.withExpr(OutputExpression(oe.ruleExpr))
                        alreadyProcessed.put(r.runOnPassProcessor.id, processed)
                        processed
                      })
                    )
                  }.getOrElse {
                    val s = notexists.getOrElse(rsuite.id, Set.empty)
                    notexists(rsuite.id) = (s + r)
                    r
                  }
                else
                  r
            })
        })

      }

    (map, Map() ++ notexists)
  }


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
                       ): Map[Id, Seq[LambdaFunction]] = {
    import lambdaFunctionDF.sparkSession.implicits._

    val lambdaFunctionRows = lambdaFunctionDF.select(
      lambdaFunctionName.as("name"),
      lambdaFunctionExpression.as("ruleExpr"),
      lambdaFunctionId.as("functionId"),
      lambdaFunctionVersion.as("functionVersion"),
      lambdaFunctionRuleSuiteId.as("ruleSuiteId"),
      lambdaFunctionRuleSuiteVersion.as("ruleSuiteVersion")
    ).as[LambdaFunctionRow].toLocalIterator().asScala.toSeq

    lambdaFunctionRows.groupBy( r => packId( Id(r.ruleSuiteId, r.ruleSuiteVersion))).mapValues {
      values =>
        val id = Id(values.head.ruleSuiteId, values.head.ruleSuiteVersion)
        id -> values.map( r => LambdaFunction(r.name, r.ruleExpr, Id(r.functionId, r.functionVersion)) ).toVector
    }.values.toMap
  }

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
                                 ): Map[Id, Seq[OutputExpressionRow]] = {
    import outputExpressionDF.sparkSession.implicits._

    val outputExpressionRows = outputExpressionDF.select(
      outputExpression.as("ruleExpr"),
      outputExpressionId.as("functionId"),
      outputExpressionVersion.as("functionVersion"),
      outputExpressionRuleSuiteId.as("ruleSuiteId"),
      outputExpressionRuleSuiteVersion.as("ruleSuiteVersion")
    ).as[OutputExpressionRow].toLocalIterator().asScala.toSeq

    outputExpressionRows.groupBy( r => packId( Id(r.ruleSuiteId, r.ruleSuiteVersion))).mapValues {
      values =>
        val id = Id(values.head.ruleSuiteId, values.head.ruleSuiteVersion)
        id -> values
    }.values.toMap
  }

  def readMetaRuleSetsFromDF(
                              metaRuleSetDF: DataFrame,
                              columnSelectionExpression: Column,
                              lambdaFunctionExpression: Column,
                              metaRuleSetId: Column,
                              metaRuleSetVersion: Column,
                              metaRuleSuiteId: Column,
                              metaRuleSuiteVersion: Column
                            ): Map[Id, Seq[MetaRuleSetRow]] = {
    import metaRuleSetDF.sparkSession.implicits._

    val lambdaFunctionRows = metaRuleSetDF.select(
      columnSelectionExpression.as("columnFilter"),
      lambdaFunctionExpression.as("ruleExpr"),
      metaRuleSetId.as("ruleSetId"),
      metaRuleSetVersion.as("ruleSetVersion"),
      metaRuleSuiteId.as("ruleSuiteId"),
      metaRuleSuiteVersion.as("ruleSuiteVersion")
    ).as[MetaRuleSetRow].toLocalIterator().asScala.toSeq

    lambdaFunctionRows.groupBy( r => packId( Id(r.ruleSuiteId, r.ruleSuiteVersion))).mapValues {
      values =>
        val id = Id(values.head.ruleSuiteId, values.head.ruleSuiteVersion)
        id -> values
    }.values.toMap
  }

  /**
   * Spark _can_ return Streams as Seq instead of Vectors or Lists, which aren't serializable and it
   * gives a crappy message, hence the call toVector, sortBy is used to aid testing
   *
   * @param ruleSuite
   * @return
   */
  def toSeq(ruleSuite: RuleSuite): RuleSuite =
    RuleSuite(ruleSuite.id,
      ruleSuite.ruleSets.map( rs => RuleSet(rs.id, rs.rules.sortBy(r => packId(r.id)).toVector)).toVector
    )

  /**
   * Calls out to toSeq for each RuleSuite
   * @param ruleSuiteMap
   * @return
   */
  def toSeq(ruleSuiteMap: RuleSuiteMap): RuleSuiteMap =
    ruleSuiteMap.mapValues(toSeq _)

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
   * Loads a RuleSuite from a dataframe with integers ruleSuiteId, ruleSuiteVersion, ruleSetId, ruleSetVersion, ruleId, ruleVersion and an expression string ruleExpr
   */
  def ireadRulesFromDF(df: DataFrame,
                       ruleSuiteId: Column,
                       ruleSuiteVersion: Column,
                       ruleSetId: Column,
                       ruleSetVersion: Column,
                       ruleId: Column,
                       ruleVersion: Column,
                       ruleExpr: Column,
                       ruleEngine: Option[(Column, Column, Column)] = None
                      ): RuleSuiteMap = {

    import df.sparkSession.implicits._

    val baseColumns =
      Seq(
        ruleSuiteId.as("ruleSuiteId"),
        ruleSuiteVersion.as("ruleSuiteVersion"),
        ruleSetId.as("ruleSetId"),
        ruleSetVersion.as("ruleSetVersion"),
        ruleId.as("ruleId"),
        ruleVersion.as("ruleVersion"),
        ruleExpr.as("ruleExpr")
      )

    val ruleRowsColumns =
      ruleEngine.map(re => baseColumns ++ Seq(
        re._1.as("ruleEngineSalience"),
        re._2.as("ruleEngineId"),
        re._3.as("ruleEngineVersion")
      )).getOrElse(
        baseColumns ++ Seq(
          expr("1234567890 as ruleEngineSalience"),
          expr("0 as ruleEngineId"),
          expr("0 as ruleEngineVersion")
        )
      )

    val ruleRows = df.select(ruleRowsColumns :_* ).as[RuleRow].toLocalIterator().asScala.toSeq

    val r = ruleRows.groupBy(r => packId( Id(r.ruleSuiteId, r.ruleSuiteVersion) )).mapValues {
      suite =>
        val id = Id(suite.head.ruleSuiteId, suite.head.ruleSuiteVersion)
        (id: VersionedId) -> RuleSuite(id,
          suite.groupBy(rs => packId(Id(rs.ruleSetId, rs.ruleSetVersion))).mapValues(
            rules =>
              RuleSet( Id(rules.head.ruleSetId, rules.head.ruleSetVersion) ,
                rules.map(rule => Rule(Id(rule.ruleId, rule.ruleVersion), ExpressionRule(rule.ruleExpr),
                  ruleEngine.map(re =>
                    RunOnPassProcessorHolder(rule.ruleEngineSalience, Id(rule.ruleEngineId, rule.ruleEngineVersion))).
                    getOrElse( NoOpRunOnPassProcessor.noOp
                    ))).toSeq
              )
          ).values.toSeq
        )
    }.values.toMap
    // force seq
    toSeq(r)
  }

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
  def toDS(ruleSuite: RuleSuite): Dataset[RuleRow] = {
    val flattened =
      ruleSuite.ruleSets.flatMap { ruleSet =>
        ruleSet.rules.map(rule =>
          (ruleSuite.id, ruleSet.id, rule.id, rule.expression match {
            case ExpressionRule(expr) => expr
            case _ => "1=1"
          }, rule.runOnPassProcessor.salience,
            rule.runOnPassProcessor.id.id,
            rule.runOnPassProcessor.id.version)
        )
      }
    val ruleRows = flattened.map{ rule =>
      RuleRow(
        rule._1.id,
        rule._1.version,
        rule._2.id,
        rule._2.version,
        rule._3.id,
        rule._3.version,
        rule._4,
        rule._5,
        rule._6,
        rule._7
      )
    }

    val sess = SparkSession.getDefaultSession.get
    import sess.implicits._

    sess.createDataset(ruleRows)
  }

  /**
   * Must have an active sparksession before calling and only works with ExpressionRule's, all other rules are converted to 1=1
   * @param ruleSuite
   * @return a Dataset[RowRaw] with the rules flattened out
   */
  def toLambdaDS(ruleSuite: RuleSuite): Dataset[LambdaFunctionRow] = {
    val flattened =
      ruleSuite.lambdaFunctions.map { function =>
        LambdaFunctionRow(function.name, function.rule, function.id.id, function.id.version, ruleSuite.id.id, ruleSuite.id.version)
      }

    val sess = SparkSession.getDefaultSession.get
    import sess.implicits._

    sess.createDataset(flattened)
  }

  /**
   * Must have an active sparksession before calling and only works with ExpressionRule's, all other rules are converted to 1=1
   * @param ruleSuite
   * @return a Dataset[RowRaw] with the rules flattened out
   */
  def toOutputExpressionDS(ruleSuite: RuleSuite): Dataset[OutputExpressionRow] = {
    val flattened =
      ruleSuite.ruleSets.flatMap(rs => rs.rules.map { rule =>
        val oe = rule.runOnPassProcessor
        OutputExpressionRow(oe.rule, oe.id.id, oe.id.version, ruleSuite.id.id, ruleSuite.id.version)
      })

    val sess = SparkSession.getDefaultSession.get
    import sess.implicits._

    sess.createDataset(flattened)
  }

}
