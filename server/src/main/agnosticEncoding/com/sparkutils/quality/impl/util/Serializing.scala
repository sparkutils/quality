package com.sparkutils.quality.impl.util

import com.sparkutils.quality.{Id, OutputExpressionRow, LambdaFunctionRow}
import com.sparkutils.quality.NoOpRunOnPassProcessor.{notPresentOutputId, notPresentOutputVersion, notPresentSalience}
import com.sparkutils.quality.RuleSuite.defaultProbablePass
import com.sparkutils.testing.ConnectWhenForced.someOrForcedConnect
import org.apache.spark.sql.functions.{col, collect_set, expr, lit, struct}
import org.apache.spark.sql.types.{ArrayType, DoubleType}
import org.apache.spark.sql.{DataFrame, Encoder, functions}

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
                                     outputExpressionRows: Option[DataFrame] = None,
                                     globalLambdaSuites: Option[DataFrame] = None,
                                     globalOutputExpressionSuites: Option[DataFrame] = None,
                                     ruleSuites: Option[DataFrame] = None): Option[DataFrame] = someOrForcedConnect {
    CombineImpl.combineImplI(ruleRows, lambdaFunctionRows, outputExpressionRows, globalLambdaSuites,
      globalOutputExpressionSuites, ruleSuites)
  }

}
