package com.sparkutils.quality.impl.util

import com.sparkutils.quality.{Id, OutputExpressionRow, LambdaFunctionRow}
import com.sparkutils.quality.NoOpRunOnPassProcessor.{notPresentOutputId, notPresentOutputVersion, notPresentSalience}
import com.sparkutils.quality.RuleSuite.defaultProbablePass
import com.sparkutils.testing.ConnectWhenForced.someOrForcedConnect
import org.apache.spark.sql.functions.{col, collect_set, expr, lit, struct}
import org.apache.spark.sql.types.{ArrayType, DoubleType}
import org.apache.spark.sql.{DataFrame, Encoder, functions}

/**
 * Separate source file and object to allow debugging from other applications
 */
protected[quality] object CombineImpl extends GeneratedUniqueName {
  protected val GENERATED_NAME_PREFIX = "QUALITY_SERIALIZING_SHIM_GENERATED_NAME_"

  /**
   * combine implementation for loading CombinedRules, this is usable by all jvm languages and, by default, expects a server extension for pure quality_api users.
   * the server implementation will provide a direct call (as it's possible no extension is present)
   *
   * @param ruleRows
   * @param lambdaFunctionRows
   * @param outputExpressionRows
   * @param globalLambdaSuites
   * @param globalOutputExpressionSuites
   * @return a combined dataframe
   */
  protected[quality] def combineImplI(ruleRows: DataFrame, lambdaFunctionRows: Option[DataFrame] = None,
                                     outputExpressionRows: Option[DataFrame] = None,
                                     globalLambdaSuites: Option[DataFrame] = None,
                                     globalOutputExpressionSuites: Option[DataFrame] = None,
                                     ruleSuites: Option[DataFrame] = None): DataFrame = {

    import ruleRows.sparkSession.implicits._

    val outputExpressionRowsT = outputExpressionRows.flatMap(r => if (r.isEmpty) None else Some(r))
    val lambdaFunctionRowsT = lambdaFunctionRows.flatMap(r => if (r.isEmpty) None else Some(r))

    val groupedLambdasN = uniqueName()
    val outsN = uniqueName()
    outputExpressionRowsT.fold(ruleRows.sparkSession.createDataset[OutputExpressionRow](Seq.empty).toDF())(identity).
      createOrReplaceTempView(outsN)

    val lun = uniqueName()
    val oun = uniqueName()
    globalLambdaSuites.fold(ruleRows.sparkSession.createDataset[Id](Seq.empty).toDF())(identity).
      createOrReplaceTempView(lun)
    globalOutputExpressionSuites.fold(ruleRows.sparkSession.createDataset[Id](Seq.empty).toDF())(identity).
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
    // TODO use the rest of the ruleSuites
    val probablePassLit = lit(defaultProbablePass).cast(DoubleType).as("probablePass")
    val defaultProcessorLit = lit(null).cast(implicitly[Encoder[OutputExpressionRow]].schema).as("defaultProcessor")

    val grouped = rows.groupBy("ruleRow.ruleSuiteId", "ruleRow.ruleSuiteVersion").agg(
      collect_set(struct(col("ruleRow"), col("outputExpressionRow"))).as("ruleRows"))
    val suiteRows =
      grouped.select(
        col("ruleSuiteId"),
        col("ruleSuiteVersion"),
        col("ruleRows"),
        lit(null).cast(ArrayType(implicitly[Encoder[LambdaFunctionRow]].schema)).as("lambdaFunctions"), // Using Seq in the implicit treats it as a valueclass of seq.
        probablePassLit,
        defaultProcessorLit
      )

    val withAttributes =
      ruleSuites.fold(suiteRows) {
        rsa =>

          val adjusted =
            rsa.select(col("ruleSuiteId").as("rRuleSuiteId"), col("ruleSuiteVersion").as("rRuleSuiteVersion"),
              col("probablePass").as("rProbablePass"),
              functions.when(
                lit(notPresentOutputId) === col("ruleEngineId") &&
                  lit(notPresentOutputVersion) === col("ruleEngineVersion")
                , defaultProcessorLit).
                otherwise(
                  expr(
                    s"""
                      (select first(struct(ruleExpr, functionId, functionVersion, ruleSuiteId, ruleSuiteVersion))
                          from $outsN outs
                          where (
                            outs.ruleSuiteId = ruleSuiteId and outs.ruleSuiteVersion = ruleSuiteVersion
                            or (exists (
                             select 0 from $oun gls
                             where gls.id = outs.ruleSuiteId and gls.version = outs.ruleSuiteVersion
                            ) )
                          ) and outs.functionId = ruleEngineId and outs.functionVersion = ruleEngineVersion
                       )
                      """
                  )
                ).as("rDefaultProcessor")
            )

          suiteRows.join(adjusted,
            col("ruleSuiteId") === col("rRuleSuiteId") && col("ruleSuiteVersion") === col("rRuleSuiteVersion")
          ).select(
            col("ruleSuiteId"),
            col("ruleSuiteVersion"),
            col("ruleRows"),
            col("lambdaFunctions"),
            col("rProbablePass").as("probablePass"),
            col("rDefaultProcessor").as("defaultProcessor")
          )
      }

    val withLambdas =
      lambdaFunctionRowsT.fold(withAttributes){
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
            col("theLambdaFunctions")).as("grouped")

          grouped.createOrReplaceTempView(groupedLambdasN)

          withAttributes.select(
            col("ruleSuiteId"),
            col("ruleSuiteVersion"),
            col("ruleRows"),
            expr(
              s"""(select flatten(collect_set(theLambdaFunctions))
                  from $groupedLambdasN grouped
                  where grouped.lRuleSuiteId = ruleSuiteId and grouped.lRuleSuiteVersion = ruleSuiteVersion
                  or (exists (
                   select 0 from $lun gls
                   where gls.id = grouped.lRuleSuiteId and gls.version = grouped.lRuleSuiteVersion
                  ) )
               )""").as("lambdaFunctions")
            , col("probablePass"),
            col("defaultProcessor")
          )
      }

    withLambdas
  }

}
