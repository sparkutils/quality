package com.sparkutils.quality.impl.util

import com.sparkutils.quality.{ExpressionRule, Id, Rule, RuleSet, RuleSuite}
import com.sparkutils.quality.impl.VersionedId
import org.apache.spark.sql.DataFrame

object RuleModel {
  /**
   * Returned from RuleSuite loading
   */
  type RuleSuiteMap = Map[VersionedId, RuleSuite]
}

case class RuleRow(ruleSuiteId: Int,
                   ruleSuiteVersion: Int,
                   ruleSetId: Int,
                   ruleSetVersion: Int,
                   ruleId: Int,
                   ruleVersion: Int,
                   ruleExpr: String,
                   ruleEngineSalience: Int = Int.MaxValue,
                   ruleEngineId: Int = 0,
                   ruleEngineVersion: Int = 0
                  )

case class LambdaFunctionRow(name: String, ruleExpr: String, functionId: Int,
                             functionVersion: Int, ruleSuiteId: Int,
                             ruleSuiteVersion: Int)

case class OutputExpressionRow(ruleExpr: String, functionId: Int,
                               functionVersion: Int, ruleSuiteId: Int,
                               ruleSuiteVersion: Int)

/**
 * Used to filter columns for meta RuleSets
 * @param name
 * @param dataType
 * @param nullable
 */
case class SimpleField(name: String, dataType: String, nullable: Boolean)

/**
 * only one arg is supported without brackets etc.
 *
 * Law: Each Rule generated must have a stable Id for the same column, the version used is the same as the RuleSet
 *
 * The caller of withNameAndOrd must enforce this law to have stable and correctly evolving rules.
 *
 * @param ruleSuiteId
 * @param ruleSuiteVersion
 * @param ruleSetId
 * @param ruleSetVersion
 * @param columnFilter
 * @param ruleExpr
 */
case class MetaRuleSetRow(ruleSuiteId: Int,
                          ruleSuiteVersion: Int,
                          ruleSetId: Int,
                          ruleSetVersion: Int,
                          columnFilter: String,
                          ruleExpr: String) {
  val arg = ruleExpr.split("->")(0).trim
  val rulePart = ruleExpr.split("->")(1).trim
  //val columnFilterExp = RuleLogicUtils.expr(columnFilter)

  /**
   * @param stablePosition - should always be the same for this column, evaluated with columnName
   *
   */
  protected[quality] case class MetaRuleGenerator( stablePosition: String => Int) {
    /**
     * Create a new rule - this must obey the law stated above
     *
     * @param columnName     - will be lowercase fed by filterColumns
     * @return
     */
    def withNameAndOrd(columnName: String) =
      Rule(Id(ruleSetId + stablePosition(columnName), ruleSetVersion),
        ExpressionRule(rulePart.replaceAll(s"\\b$arg\\b", columnName)))
  }

  /**
   * Applies a filter to the schema of the provided dataframe
   * @param dataFrame
   * @return a lower case set of column names
   */
  protected[quality] def filterColumns(dataFrame: DataFrame, transform: DataFrame => DataFrame = identity): Set[String] = {
    import dataFrame.sparkSession
    import sparkSession.implicits._

    val origds = sparkSession.createDataset( dataFrame.schema.fields.map(f=>
      SimpleField(f.name, f.dataType.sql, f.nullable)) ).toDF()
    val ds = transform(origds)
      .filter(columnFilter)
    ds.collect().map(_.getAs[String]("name")).map(_.toLowerCase).toSet
  }

  /**
   * creates the generator used to make rules
   * @param stablePosition
   * @return
   */
  protected[quality] def createGenerator(stablePosition: String => Int) = MetaRuleGenerator(stablePosition)

  /**
   * Generates rules over a given dataframe using the stablePosition function
   * @param dataFrame
   * @param stablePosition - should always be the same for this column, evaluated with columnName
   * @param transform - allows enriching of the column, e.g. joining another table for extra filtering information.
   * @return
   */
  def generateRuleSet(dataFrame: DataFrame, stablePosition: String => Int, transform: DataFrame => DataFrame = identity): RuleSet = RuleSet(Id(ruleSetId, ruleSetVersion),{
    val cols = filterColumns(dataFrame, transform).toSeq // remove dupes, but stay as seq
    val gen = createGenerator(stablePosition)
    import gen.withNameAndOrd
    cols.map(withNameAndOrd)
  })
}

/**
 * Flattened results for aggregation / display / use by the explodeResults expression
 */
case class RuleResultRow(ruleSuiteId: Int,
                         ruleSuiteVersion: Int,
                         ruleSuiteResult: Int,
                         ruleSetResult: Int,
                         ruleSetId: Int,
                         ruleSetVersion: Int,
                         ruleId: Int,
                         ruleVersion: Int,
                         ruleResult: Int)
