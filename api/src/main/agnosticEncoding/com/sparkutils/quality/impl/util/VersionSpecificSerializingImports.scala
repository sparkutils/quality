package com.sparkutils.quality.impl.util

import com.sparkutils.quality.impl.{RuleSuiteHelpers, VariableHelper}
import com.sparkutils.quality.{ExpressionRule, Id, LambdaFunction, OutputExpression, Rule, RuleSet, RuleSuite, RunOnPassProcessor, NoOpRunOnPassProcessor, VersionedId}
import com.sparkutils.quality.impl.util.Serializing.{notPresentOutputId, notPresentOutputVersion, notPresentSalience}
import com.sparkutils.quality.impl.util.VersionSpecificSerializingImports.uniqueName
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.sql.functions.{col, collect_set, expr, lit, struct}
import org.apache.spark.sql.types.{ArrayType, BinaryType, DoubleType}

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
case class CombinedRuleSuiteRows(ruleSuiteId: Int, ruleSuiteVersion: Int, ruleRows: Seq[CombinedRuleRow], lambdaFunctions: Option[Seq[LambdaFunctionRow]], probablePass: Option[Double])

object VersionSpecificSerializingImports {

  private val nameCounter = new AtomicInteger(0)

  private val GENERATED_NAME_PREFIX = "QUALITY_RULE_SUITE_GENERATED_NAME_"

  // only for the current session, so regardless of on driver with static or connect client this works
  protected[quality] def uniqueName(): String = GENERATED_NAME_PREFIX + nameCounter.incrementAndGet()

}

trait VersionSpecificSerializingImports {
  // todo simpleVersioning needs to be done as well
  // todo the ClassicOnly / ConnectFriendly annotations?  Does it make sense if there is a split connect jar?  The same
  // would work in classic though

  /**
   *
   * @param ruleRows
   * @param lambdaFunctionRows
   * @param outputExpressionRows
   * @param probablePass
   * @param globalLambdaSuites each suite present is also integrated with every ruleSuite used in ruleRows - use filters to restrict the namespace being polluted
   * @param globalOutputExpressionSuites each suite is applied / available to each suite found in the ruleRows, with functionId and functionVersion as joins
   * @return
   */
  private def icombine(ruleRows: Dataset[RuleRow], lambdaFunctionRows: Option[Dataset[LambdaFunctionRow]] = None,
              outputExpressionRows: Option[Dataset[OutputExpressionRow]] = None, probablePass: Option[Double] = None,
              globalLambdaSuites: Option[Dataset[Id]] = None, globalOutputExpressionSuites: Option[Dataset[Id]] = None): Dataset[CombinedRuleSuiteRows] = {

    import ruleRows.sparkSession.implicits._

    val outputExpressionRowsT = outputExpressionRows.flatMap(r => if (r.isEmpty) None else Some(r))
    val lambdaFunctionRowsT = lambdaFunctionRows.flatMap(r => if (r.isEmpty) None else Some(r))

    val lun = uniqueName()
    val oun = uniqueName()
    globalLambdaSuites.fold(ruleRows.sparkSession.createDataset[Id](Seq.empty))(identity).
      createOrReplaceTempView(lun)
    globalOutputExpressionSuites.fold(ruleRows.sparkSession.createDataset[Id](Seq.empty))(identity).
      createOrReplaceTempView(oun)

    val rows = outputExpressionRowsT.fold(ruleRows.select(
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
            ((ruleRows("ruleSuiteId") === col("oRuleSuiteId") &&
            ruleRows("ruleSuiteVersion") === col("oRuleSuiteVersion")) ||
              // it's global
              expr(
                s"""(exists (
                      select 0 from $oun goes
                      where goes.id = oRuleSuiteId and goes.version = oRuleSuiteVersion
                    ))
                   """) ) &&
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

    val probablePassLit = probablePass.map(lit(_)).getOrElse(lit(null).cast(DoubleType)).as("probablePass")

    val grouped = rows.groupBy("ruleRow.ruleSuiteId", "ruleRow.ruleSuiteVersion").agg(
      collect_set(struct(col("ruleRow"), col("outputExpressionRow"))).as("ruleRows"))
    val suiteRows =
      grouped.select(
        col("ruleSuiteId"),
        col("ruleSuiteVersion"),
        col("ruleRows"),
        lit(null).cast(ArrayType(implicitly[Encoder[LambdaFunctionRow]].schema)).as("lambdaFunctions"), // Using Seq in the implicit treats it as a valueclass of seq.
        probablePassLit
      )

    lambdaFunctionRowsT.fold(suiteRows){
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
        ).as("theLambdaFunctions")).select(col("ruleSuiteId").as("lRuleSuiteId"),
          col("ruleSuiteVersion").as("lRuleSuiteVersion"),
          col("theLambdaFunctions"), probablePassLit)
       suiteRows.join(grouped,
           (suiteRows("ruleSuiteId") === grouped("lRuleSuiteId") &&
             suiteRows("ruleSuiteVersion") === grouped("lRuleSuiteVersion") ) ||
             // it's global
             expr(
               s"""(exists (
                      select 0 from $lun gls
                      where gls.id = lRuleSuiteId and gls.version = lRuleSuiteVersion
                    ))
                   """)
         ).
         select(
           col("ruleSuiteId"),
           col("ruleSuiteVersion"),
           col("ruleRows"),
           col("theLambdaFunctions").as("lambdaFunctions"),
           probablePassLit
         )
    }.as[CombinedRuleSuiteRows]
  }

  /**
   * Combines ruleRows, lambdaFunctionRows and outputExpressionRows into CombinedRuleSuiteRows
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
   * Combines ruleRows and lambdaFunctionRows into CombinedRuleSuiteRows
   * @param ruleRows
   * @param lambdaFunctionRows
   * @return
   */
  def combine(ruleRows: Dataset[RuleRow], lambdaFunctionRows: Dataset[LambdaFunctionRow]): Dataset[CombinedRuleSuiteRows] =
    icombine(ruleRows = ruleRows, lambdaFunctionRows = Some(lambdaFunctionRows))

  /**
   * Uses ruleRows to make CombinedRuleSuiteRows
   * @param ruleRows
   * @return
   */
  def combine(ruleRows: Dataset[RuleRow]): Dataset[CombinedRuleSuiteRows] =
    icombine(ruleRows = ruleRows)

  /**
   * Combines ruleRows, lambdaFunctionRows and outputExpressionRows into CombinedRuleSuiteRows, using probablePass
   * @param ruleRows
   * @param lambdaFunctionRows
   * @param outputExpressionRows
   * @param probablePass
   * @return
   */
  def combine(ruleRows: Dataset[RuleRow], lambdaFunctionRows: Dataset[LambdaFunctionRow],
              outputExpressionRows: Dataset[OutputExpressionRow], probablePass: Double): Dataset[CombinedRuleSuiteRows] =
    icombine(ruleRows = ruleRows, lambdaFunctionRows = Some(lambdaFunctionRows),
      outputExpressionRows = Some(outputExpressionRows), probablePass = Some(probablePass))

  /**
   * Combines ruleRows, lambdaFunctionRows and outputExpressionRows into CombinedRuleSuiteRows, using probablePass
   * @param ruleRows
   * @param lambdaFunctionRows
   * @param outputExpressionRows
   * @param probablePass
   * @return
   */
  def combine(ruleRows: Dataset[RuleRow], lambdaFunctionRows: Dataset[LambdaFunctionRow],
              outputExpressionRows: Dataset[OutputExpressionRow], probablePass: Double,
              globalLambdaSuites: Dataset[Id], globalOutputExpressionSuites: Dataset[Id]): Dataset[CombinedRuleSuiteRows] =
    icombine(ruleRows = ruleRows, lambdaFunctionRows = Some(lambdaFunctionRows),
      outputExpressionRows = Some(outputExpressionRows), probablePass = Some(probablePass),
      globalLambdaSuites = Some(globalLambdaSuites), globalOutputExpressionSuites = Some(globalOutputExpressionSuites)
    )


  /**
   * Combines ruleRows, lambdaFunctionRows and outputExpressionRows into CombinedRuleSuiteRows, using probablePass
   * @param ruleRows
   * @param lambdaFunctionRows
   * @param outputExpressionRows
   * @param probablePass
   * @return
   */
  def combine(ruleRows: Dataset[RuleRow], lambdaFunctionRows: Dataset[LambdaFunctionRow],
              outputExpressionRows: Dataset[OutputExpressionRow],
              globalLambdaSuites: Dataset[Id], globalOutputExpressionSuites: Dataset[Id]): Dataset[CombinedRuleSuiteRows] =
    icombine(ruleRows = ruleRows, lambdaFunctionRows = Some(lambdaFunctionRows),
      outputExpressionRows = Some(outputExpressionRows),
      globalLambdaSuites = Some(globalLambdaSuites), globalOutputExpressionSuites = Some(globalOutputExpressionSuites)
    )

  /**
   * Combines ruleRows, lambdaFunctionRows and outputExpressionRows into CombinedRuleSuiteRows, using probablePass
   * @param ruleRows
   * @param lambdaFunctionRows
   * @param outputExpressionRows
   * @param probablePass
   * @return
   */
  def combine(ruleRows: Dataset[RuleRow], lambdaFunctionRows: Dataset[LambdaFunctionRow],
              outputExpressionRows: Dataset[OutputExpressionRow], probablePass: Double,
              globalLambdaSuites: Dataset[Id]): Dataset[CombinedRuleSuiteRows] =
    icombine(ruleRows = ruleRows, lambdaFunctionRows = Some(lambdaFunctionRows),
      outputExpressionRows = Some(outputExpressionRows), probablePass = Some(probablePass),
      globalLambdaSuites = Some(globalLambdaSuites)
    )

  /**
   * Combines ruleRows and lambdaFunctionRows into CombinedRuleSuiteRows, using probablePass
   * @param ruleRows
   * @param lambdaFunctionRows
   * @param probablePass
   * @return
   */
  def combine(ruleRows: Dataset[RuleRow], lambdaFunctionRows: Dataset[LambdaFunctionRow], probablePass: Double): Dataset[CombinedRuleSuiteRows] =
    icombine(ruleRows = ruleRows, lambdaFunctionRows = Some(lambdaFunctionRows), probablePass = Some(probablePass))

  /**
   * Converts ruleRows into CombinedRuleSuiteRows, using probablePass
   * @param ruleRows
   * @param probablePass
   * @return
   */
  def combine(ruleRows: Dataset[RuleRow], probablePass: Double): Dataset[CombinedRuleSuiteRows] =
    icombine(ruleRows = ruleRows, probablePass = Some(probablePass))

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
      s"(select first(struct(ruleSuiteId, ruleSuiteVersion, ruleRows, lambdaFunctions, probablePass)) from `$tv`)")

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


