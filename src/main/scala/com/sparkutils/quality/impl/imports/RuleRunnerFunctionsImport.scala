package com.sparkutils.quality.impl.imports

import com.sparkutils.quality.impl.RuleRegistrationFunctions
import com.sparkutils.quality.impl.extension.FunNRewrite
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.qualityFunctions.utils
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{QualitySparkUtils, ShimUtils, SparkSession}

trait RuleRunnerFunctionsImport {

  import RuleRegistrationFunctions._

  /**
   * Must be called before using any functions like Passed, Failed or Probability(X)
   * @param parseTypes override type parsing (e.g. DDL, defaults to defaultParseTypes / DataType.fromDDL)
   * @param zero override zero creation for aggExpr (defaults to defaultZero)
   * @param add override the "add" function for aggExpr types (defaults to defaultAdd(dataType))
   * @param writer override the printCode and printExpr print writing function (defaults to println)
   * @param registerFunction function to register the sql extensions
   */
  def registerQualityFunctions(parseTypes: String => Option[DataType] = defaultParseTypes _,
                               zero: DataType => Option[Any] = defaultZero _,
                               add: DataType => Option[(Expression, Expression) => Expression] = (dataType: DataType) => defaultAdd(dataType),
                               mapCompare: DataType => Option[(Any, Any) => Int] = (dataType: DataType) => utils.defaultMapCompare(dataType),
                               writer: String => Unit = println(_),
                               registerFunction: (String, Seq[Expression] => Expression) => Unit =
                                  ShimUtils.registerFunction(SparkSession.getActiveSession.get.sessionState.functionRegistry) _
                       ) =
    RuleRegistrationFunctions.registerQualityFunctions(parseTypes,
      zero,
      add,
      mapCompare,
      writer,
      registerFunction)

  /**
   * Enables the FunNRewrite optimisation. Where a user configured LambdaFunction does not have nested
   * HigherOrderFunctions, or declares the `/* USED_AS_LAMBDA */` comment, the lambda function will be expanded,
   * replacing all LambdaVariables with the input expressions.
   */
  def enableFunNRewrites(): Unit = {
    SparkSession.getActiveSession.get.experimental.extraOptimizations =
      SparkSession.getActiveSession.get.experimental.extraOptimizations :+ FunNRewrite
  }
}
