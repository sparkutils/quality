package com.sparkutils.quality.impl.util

import com.sparkutils.quality.impl.{RuleSuiteHelpers, VariableHelper}
import com.sparkutils.quality.{ExpressionRule, Id, LambdaFunction, NoOpRunOnPassProcessor, OutputExpression, Rule, RuleSet, RuleSuite, RunOnPassProcessor, VersionedId}
import com.sparkutils.quality.impl.util.SerializingShim.combineImpl
import com.sparkutils.quality.impl.util.VersionSpecificSerializingImports.uniqueName
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.BinaryType

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
case class CombinedRuleSuiteRows(ruleSuiteId: Int, ruleSuiteVersion: Int, ruleRows: Seq[CombinedRuleRow], lambdaFunctions: Option[Seq[LambdaFunctionRow]], probablePass: Option[Double], defaultProcessor: Option[OutputExpressionRow])

object VersionSpecificSerializingImports extends GeneratedUniqueName {
  protected val GENERATED_NAME_PREFIX = "QUALITY_RULE_SUITE_GENERATED_NAME_"
}

trait VersionSpecificSerializingImports {

  /**
   *
   * @param ruleRows
   * @param lambdaFunctionRows
   * @param outputExpressionRows
   * @param globalLambdaSuites each suite present is also integrated with every ruleSuite used in ruleRows - use filters to restrict the namespace being polluted
   * @param globalOutputExpressionSuites each suite is applied / available to each suite found in the ruleRows, with functionId and functionVersion as joins
   * @return
   */
  private def icombine(ruleRows: Dataset[RuleRow], lambdaFunctionRows: Option[Dataset[LambdaFunctionRow]] = None,
              outputExpressionRows: Option[Dataset[OutputExpressionRow]] = None,
              globalLambdaSuites: Option[Dataset[Id]] = None, globalOutputExpressionSuites: Option[Dataset[Id]] = None,
                       ruleSuites: Option[Dataset[RuleSuiteRow]] = None)(
                      implicit encoder: Encoder[CombinedRuleSuiteRows]
  ): Dataset[CombinedRuleSuiteRows] =
    combineImpl(ruleRows.toDF(), lambdaFunctionRows.map(_.toDF()), outputExpressionRows.map(_.toDF()),
      globalLambdaSuites.map(_.toDF()), globalOutputExpressionSuites.map(_.toDF()),
      ruleSuites.map(_.toDF())).orElse {
      remoteCombine(ruleRows, lambdaFunctionRows, outputExpressionRows,
        globalLambdaSuites, globalOutputExpressionSuites, ruleSuites)
    }.get.as[CombinedRuleSuiteRows]

  private def remoteCombine(ruleRows: Dataset[RuleRow], lambdaFunctionRows: Option[Dataset[LambdaFunctionRow]],
                            outputExpressionRows: Option[Dataset[OutputExpressionRow]],
                            globalLambdaSuites: Option[Dataset[Id]],
                            globalOutputExpressionSuites: Option[Dataset[Id]],
                            ruleSuites: Option[Dataset[RuleSuiteRow]]): Option[org.apache.spark.sql.DataFrame] = {
    val rname = uniqueName()
    ruleRows.createOrReplaceGlobalTempView(rname)

    val lfname = registerTempViewNameFromDS(lambdaFunctionRows)
    val oename = registerTempViewNameFromDS(outputExpressionRows)
    val glname = registerTempViewNameFromDS(globalLambdaSuites)
    val gloename = registerTempViewNameFromDS(globalOutputExpressionSuites)
    val rsname = registerTempViewNameFromDS(ruleSuites)

    val s = s"QUALITY COMBINE RULESUITES $rname, $lfname, $oename, $glname, $gloename, $rsname"
    Some(ruleRows.sparkSession.sql(s))
  }

  protected def registerTempViewNameFromDS[T](lambdaFunctionRows: Option[Dataset[T]]): String =
    lambdaFunctionRows.map {
      ds =>
        val lfname = uniqueName()
        ds.createOrReplaceGlobalTempView(lfname)
        lfname
    }.getOrElse("`None`")


  /**
   * Combines ruleRows, lambdaFunctionRows and outputExpressionRows into CombinedRuleSuiteRows
   */
  def combine(ruleRows: Dataset[RuleRow], lambdaFunctionRows: Option[Dataset[LambdaFunctionRow]] = None,
              outputExpressionRows: Option[Dataset[OutputExpressionRow]] = None,
              globalLambdaSuites: Option[Dataset[Id]] = None, globalOutputExpressionSuites: Option[Dataset[Id]] = None,
              ruleSuites: Option[Dataset[RuleSuiteRow]] = None)(
               implicit encoder: Encoder[CombinedRuleSuiteRows]
             ): Dataset[CombinedRuleSuiteRows] =
    icombine(ruleRows, lambdaFunctionRows, outputExpressionRows,
      globalLambdaSuites, globalOutputExpressionSuites,
      ruleSuites)

  /**
   * Combines ruleRows, lambdaFunctionRows and outputExpressionRows into CombinedRuleSuiteRows
   *
   * @param ruleRows
   * @param lambdaFunctionRows
   * @param outputExpressionRows
   * @return
   */
  def combine(ruleRows: Dataset[RuleRow], lambdaFunctionRows: Dataset[LambdaFunctionRow],
              outputExpressionRows: Dataset[OutputExpressionRow])
             (implicit encoder: Encoder[CombinedRuleSuiteRows]): Dataset[CombinedRuleSuiteRows] =
    icombine(ruleRows = ruleRows, lambdaFunctionRows = Some(lambdaFunctionRows),
      outputExpressionRows = Some(outputExpressionRows))

  /**
   * Combines ruleRows and lambdaFunctionRows into CombinedRuleSuiteRows
   * @param ruleRows
   * @param lambdaFunctionRows
   * @return
   */
  def combine(ruleRows: Dataset[RuleRow], lambdaFunctionRows: Dataset[LambdaFunctionRow])
             (implicit encoder: Encoder[CombinedRuleSuiteRows]): Dataset[CombinedRuleSuiteRows] =
    icombine(ruleRows = ruleRows, lambdaFunctionRows = Some(lambdaFunctionRows))

  /**
   * Uses ruleRows to make CombinedRuleSuiteRows
   * @param ruleRows
   * @return
   */
  def combine(ruleRows: Dataset[RuleRow])
             (implicit encoder: Encoder[CombinedRuleSuiteRows]): Dataset[CombinedRuleSuiteRows] =
    icombine(ruleRows = ruleRows)

  /**
   * Combines ruleRows, lambdaFunctionRows and outputExpressionRows into CombinedRuleSuiteRows, using probablePass
   * @param ruleRows
   * @param lambdaFunctionRows
   * @param outputExpressionRows
   * @return
   */
  def combine(ruleRows: Dataset[RuleRow], lambdaFunctionRows: Dataset[LambdaFunctionRow],
              outputExpressionRows: Dataset[OutputExpressionRow], ruleSuites: Dataset[RuleSuiteRow])
             (implicit encoder: Encoder[CombinedRuleSuiteRows]): Dataset[CombinedRuleSuiteRows] =
    icombine(ruleRows = ruleRows, lambdaFunctionRows = Some(lambdaFunctionRows),
      outputExpressionRows = Some(outputExpressionRows), ruleSuites = Some(ruleSuites))

  /**
   * Combines ruleRows, lambdaFunctionRows and outputExpressionRows into CombinedRuleSuiteRows
   * @param ruleRows
   * @param lambdaFunctionRows
   * @param outputExpressionRows
   * @return
   */
  def combine(ruleRows: Dataset[RuleRow], lambdaFunctionRows: Dataset[LambdaFunctionRow],
              outputExpressionRows: Dataset[OutputExpressionRow],
              globalLambdaSuites: Dataset[Id], globalOutputExpressionSuites: Dataset[Id],
              ruleSuites: Dataset[RuleSuiteRow])
             (implicit encoder: Encoder[CombinedRuleSuiteRows]): Dataset[CombinedRuleSuiteRows] =
    icombine(ruleRows = ruleRows, lambdaFunctionRows = Some(lambdaFunctionRows),
      outputExpressionRows = Some(outputExpressionRows),
      globalLambdaSuites = Some(globalLambdaSuites), globalOutputExpressionSuites = Some(globalOutputExpressionSuites),
      ruleSuites = Some(ruleSuites)
    )

  /**
   * Combines ruleRows, lambdaFunctionRows and outputExpressionRows into CombinedRuleSuiteRows
   * @param ruleRows
   * @param lambdaFunctionRows
   * @param outputExpressionRows
   * @return
   */
  def combine(ruleRows: Dataset[RuleRow], lambdaFunctionRows: Dataset[LambdaFunctionRow],
              outputExpressionRows: Dataset[OutputExpressionRow],
              globalLambdaSuites: Dataset[Id], ruleSuites: Dataset[RuleSuiteRow])
             (implicit encoder: Encoder[CombinedRuleSuiteRows]): Dataset[CombinedRuleSuiteRows] =
    icombine(ruleRows = ruleRows, lambdaFunctionRows = Some(lambdaFunctionRows),
      outputExpressionRows = Some(outputExpressionRows),
      globalLambdaSuites = Some(globalLambdaSuites), ruleSuites = Some(ruleSuites)
    )

  /**
   * Returns a specific RuleSuite from available rule suites
   * @param ds
   * @param id
   * @return
   */
  def rule_suite(row: CombinedRuleSuiteRows): RuleSuite = {
    RuleSuite(Id(row.ruleSuiteId, row.ruleSuiteVersion),
      row.ruleRows.groupBy(r => Id(r.ruleRow.ruleSetId, r.ruleRow.ruleSetVersion) ).map{
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

  /**
   * Registers the RuleSuite with a Spark Variable with a unique_id
   * @param ds
   * @param id
   * @return the variable name
   */
  def register_rule_suite_variable(ds: Dataset[CombinedRuleSuiteRows], id: VersionedId): String =
    register_rule_suite_variable(ds, id, uniqueName())

  /**
   * Registers the RuleSuite with a Spark Variable with the provided stable id
   * @param ds
   * @param id
   * @param stableName
   * @return stableName
   */
  def register_rule_suite_variable(ds: Dataset[CombinedRuleSuiteRows], id: VersionedId, stableName: String): String = {
    val r = ds.filter(col("ruleSuiteId") === id.id && col("ruleSuiteVersion") === id.version)
    val tv = uniqueName()
    r.createOrReplaceTempView(tv)
    val s = SparkSession.active
    import s.implicits._
    val ddl = implicitly[Encoder[CombinedRuleSuiteRows]].schema.toDDL

    VariableHelper.createVar(stableName, s"struct<$ddl>",
      s"(select first(struct(ruleSuiteId, ruleSuiteVersion, ruleRows, lambdaFunctions, probablePass, defaultProcessor)) from `$tv`)")

    stableName
  }

  /**
   * Registers a ruleSuite directly as a Spark Variable (with object stream encoding).
   * Where possible using the CombinedRuleSuiteRows should be preferred and manage the ruleSuites on the server.
   * @param ruleSuite
   * @param stableName
   * @return
   */
  def register_rule_suite(ruleSuite: RuleSuite, stableName: String): String = {
    val s = SparkSession.active
    import s.implicits._
    val tv = uniqueName()
    s.sql("select 1").select(lit(RuleSuiteHelpers.serialize(ruleSuite)).as("rs")).createOrReplaceTempView(tv)
    VariableHelper.createVar(stableName, BinaryType.sql,
      s"(select first(rs) from `$tv`)")

    stableName
  }

  /**
   * Registers a ruleSuite directly as an Spark Variable (with object stream encoding)
   * @param ruleSuite
   * @return
   */
  def register_rule_suite(ruleSuite: RuleSuite): String = register_rule_suite(ruleSuite, uniqueName())
}


