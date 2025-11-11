package com.sparkutils.quality.impl.util

import com.sparkutils.quality.{ExpressionRule, Id, LambdaFunction, OutputExpression, Rule, RuleSet, RuleSuite, RunOnPassProcessor}
import com.sparkutils.quality.impl.{LambdaFunction, NoOpRunOnPassProcessor, VersionedId}
import com.sparkutils.quality.impl.util.Serializing.{notPresentOutputId, notPresentOutputVersion, notPresentSalience}
import com.sparkutils.quality.impl.util.VersionSpecificSerializingImports.uniqueName
import org.apache.spark.sql.{Dataset, Encoder}
import org.apache.spark.sql.functions.{col, collect_set, lit, named_struct, struct}
import org.apache.spark.sql.types.ArrayType

import java.util.concurrent.atomic.AtomicInteger

/**
 * Raw model for Variable usage
 *
 * @param ruleRow
 * @param outputExpressionRow
 */
case class CombinedRuleRow(ruleRow: RuleRow, outputExpressionRow: Option[OutputExpressionRow])

/**
 * Raw model for Variable usage
 * @param ruleRows
 * @param lambdaFunctions
 */
case class CombinedRuleSuiteRows(ruleSuiteId: Int, ruleSuiteVersion: Int, ruleRows: Seq[CombinedRuleRow], lambdaFunctions: Option[Seq[LambdaFunctionRow]])

object VersionSpecificSerializingImports {

  private val nameCounter = new AtomicInteger(0)

  private val GENERATED_NAME_PREFIX = "QUALITY_RULE_SUITE_GENERATED_NAME_"

  // only for the current session, so regardless of on driver with static or connect client this works
  private def uniqueName() = GENERATED_NAME_PREFIX + nameCounter.incrementAndGet()

}

trait VersionSpecificSerializingImports {

  private def icombine(ruleRows: Dataset[RuleRow], lambdaFunctionRows: Option[Dataset[LambdaFunctionRow]] = None,
              outputExpressionRows: Option[Dataset[OutputExpressionRow]] = None): Dataset[CombinedRuleSuiteRows] = {

    import ruleRows.sparkSession.implicits._

    val rows = outputExpressionRows.fold(ruleRows.select(
      struct(
        col("ruleSuiteId"),
        col("ruleSuiteVersion"),
        col("ruleSetId"),
        col("ruleSetVersion"),
        col("ruleId"),
        col("ruleVersion"),
        col("ruleExpr"),
        lit(notPresentSalience).as("ruleEngineSalience"),
        lit(notPresentOutputId).as("ruleEngineId"),
        lit(notPresentOutputVersion).as("ruleEngineVersion")
      ).as("ruleRow"), lit(null).cast(implicitly[org.apache.spark.sql.Encoder[OutputExpressionRow]].schema).as("outputExpressionRow")))(
        outputExpressionRows =>
          // join on all fields, then apply
          ruleRows.join(outputExpressionRows.selectExpr("ruleExpr as outputRuleExpr",
            "ruleSuiteId as oRuleSuiteId", "ruleSuiteVersion as oRuleSuiteVersion", "functionId", "functionVersion"
          ),
            ruleRows("ruleSuiteId") === col("oRuleSuiteId") &&
            ruleRows("ruleSuiteVersion") === col("oRuleSuiteVersion") &&
            ruleRows("ruleEngineId") === col("functionId") &&
            ruleRows("ruleEngineVersion") === col("functionVersion")
          ).select(
            struct(
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
            ).as("ruleRow"), struct(
              col("outputRuleExpr").as("ruleExpr"),
              col("functionId"),
              col("functionVersion"),
              col("ruleSuiteId"),
              col("ruleSuiteVersion")
            ).as("outputExpressionRow")
          )
      )

    val grouped = rows.groupBy("ruleRow.ruleSuiteId", "ruleRow.ruleSuiteVersion").agg(
      collect_set(struct(col("ruleRow"), col("outputExpressionRow"))).as("ruleRows"))
    val suiteRows =
      grouped.select(
        col("ruleSuiteId"),
        col("ruleSuiteVersion"),
        col("ruleRows"),
        lit(null).cast(ArrayType(implicitly[Encoder[LambdaFunctionRow]].schema)).as("lambdaFunctions") // Using Seq in the implicit treats it as a valueclass of seq.
      )

    lambdaFunctionRows.fold(suiteRows){
      lambdas =>
        val grouped = lambdas.groupBy("ruleSuiteId", "ruleSuiteVersion").agg(collect_set(
          struct(
            col("name"),
            col("ruleExpr"),
            col("functionId"),
            col("functionVersion"),
            col("ruleSuiteId"),
            col("ruleSuiteVersion")
          )
        ).as("theLambdaFunctions")).selectExpr("ruleSuiteId as lRuleSuiteId", "ruleSuiteVersion as lRuleSuiteVersion",
          "theLambdaFunctions")
       suiteRows.join(grouped, suiteRows("ruleSuiteId") === grouped("lRuleSuiteId") &&
         suiteRows("ruleSuiteVersion") === grouped("lRuleSuiteVersion")).
         select(
           col("ruleSuiteId"),
           col("ruleSuiteVersion"),
           col("ruleRows"),
           col("theLambdaFunctions").as("lambdaFunctions")
         )
    }.as[CombinedRuleSuiteRows]
  }

  /**
   * Combines
   * @param ruleRows
   * @param lambdaFunctionRows
   * @param outputExpressionRows
   * @return
   */
  def combine(ruleRows: Dataset[RuleRow], lambdaFunctionRows: Dataset[LambdaFunctionRow],
              outputExpressionRows: Dataset[OutputExpressionRow]): Dataset[CombinedRuleSuiteRows] =
    icombine(ruleRows = ruleRows, lambdaFunctionRows = Some(lambdaFunctionRows),
      outputExpressionRows = Some(outputExpressionRows))

  /**
   * Combines
   * @param ruleRows
   * @param lambdaFunctionRows
   * @param outputExpressionRows
   * @return
   */
  def combine(ruleRows: Dataset[RuleRow], lambdaFunctionRows: Dataset[LambdaFunctionRow]): Dataset[CombinedRuleSuiteRows] =
    icombine(ruleRows = ruleRows, lambdaFunctionRows = Some(lambdaFunctionRows))

  /**
   * Combines
   * @param ruleRows
   * @param lambdaFunctionRows
   * @param outputExpressionRows
   * @return
   */
  def combine(ruleRows: Dataset[RuleRow]): Dataset[CombinedRuleSuiteRows] =
    icombine(ruleRows = ruleRows)

  /**
   * Returns a specific RuleSuite from available rule suites
   * @param ds
   * @param id
   * @return
   */
  def rule_suite(row: CombinedRuleSuiteRows): RuleSuite = {
    RuleSuite(Id(row.ruleSuiteId, row.ruleSuiteVersion),
      row.ruleRows.groupBy(r => Id(r.ruleRow.ruleSetId, r.ruleRow.ruleSuiteVersion) ).map{
        p =>
          RuleSet(p._1, p._2.map{ cr =>
            Rule(Id(cr.ruleRow.ruleId, cr.ruleRow.ruleVersion), ExpressionRule(cr.ruleRow.ruleExpr),
              runOnPassProcessor =
                cr.outputExpressionRow.map(or => RunOnPassProcessor(cr.ruleRow.ruleEngineSalience,
                    Id(or.functionId, or.functionVersion), OutputExpression(or.ruleExpr))).
                  getOrElse(NoOpRunOnPassProcessor.noOp)
            )
          })
      }.toSeq, lambdaFunctions = row.lambdaFunctions.fold(Seq.empty[LambdaFunction]){_.map { lr =>
        LambdaFunction(lr.name, lr.ruleExpr, Id(lr.functionId, lr.functionVersion))
      }})
  }

  /**
   * Returns a specific RuleSuite from available rule suites
   * @param ds
   * @param id
   * @return
   */
  def rule_suite(ds: Dataset[CombinedRuleSuiteRows], id: VersionedId): Option[RuleSuite] = {
    val r = ds.filter(col("ruleSuiteId") === id.id && col("ruleSuiteVersion") === id.version)
    if (r.isEmpty)
      None
    else
      Some(rule_suite(r.head()))
  }
}
