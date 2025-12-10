package com.sparkutils.quality.impl.util

import com.sparkutils.quality.Id
import com.sparkutils.quality.NoOpRunOnPassProcessor.{notPresentOutputId, notPresentOutputVersion, notPresentSalience}
import com.sparkutils.quality.impl.util.VersionSpecificSerializingImports.uniqueName
import com.sparkutils.testing.ConnectWhenForced.someOrForcedConnect
import org.apache.spark.sql.functions.{col, collect_set, expr, lit, struct}
import org.apache.spark.sql.types.{ArrayType, DoubleType}
import org.apache.spark.sql.{DataFrame, Encoder}

protected[quality] object SerializingShim {

  /**
   * combine implementation for loading CombinedRules, this is usable by all jvm languages and, by default, expects a server extension for pure quality_api users.
   * the server implementation will provide a direct call (as it's possible no extension is present)
   *
   * @param ruleRows
   * @param lambdaFunctionRows
   * @param outputExpressionRows
   * @param probablePass
   * @param globalLambdaSuites
   * @param globalOutputExpressionSuites
   * @return a combined dataframe
   */
  protected[quality] def combineImpl(ruleRows: DataFrame, lambdaFunctionRows: Option[DataFrame] = None,
                         outputExpressionRows: Option[DataFrame] = None, probablePass: Option[Double] = None,
                         globalLambdaSuites: Option[DataFrame] = None,
                         globalOutputExpressionSuites: Option[DataFrame] = None): Option[DataFrame] = someOrForcedConnect {

    import ruleRows.sparkSession.implicits._

    val outputExpressionRowsT = outputExpressionRows.flatMap(r => if (r.isEmpty) None else Some(r))
    val lambdaFunctionRowsT = lambdaFunctionRows.flatMap(r => if (r.isEmpty) None else Some(r))

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
          col("theLambdaFunctions"), probablePassLit).as("grouped")

        suiteRows.join(grouped,
            (suiteRows("ruleSuiteId") === col("grouped.lRuleSuiteId") &&
              suiteRows("ruleSuiteVersion") === col("grouped.lRuleSuiteVersion") ) ||
              // it's global
              expr(
                s"""(exists (
                      select 0 from $lun gls
                      where gls.id = grouped.lRuleSuiteId and gls.version = grouped.lRuleSuiteVersion
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
    }

  }

}
