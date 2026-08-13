package com.sparkutils.quality.impl.util

import org.apache.spark.sql.DataFrame

protected[quality] object SerializingShim {

  /**
   * combine implementation for loading CombinedRules, this is usable by all jvm languages and, by default, expects a server extension for pure quality_api users.
   * the server implementation will provide a direct call (as it's possible no extension is present)
   * @param ruleRows
   * @param lambdaFunctionRows
   * @param outputExpressionRows
   * @param probablePass
   * @param globalLambdaSuites
   * @param globalOutputExpressionSuites
   * @return a combined dataframe when on server, none in this stub
   */
  def combineImpl(ruleRows: DataFrame, lambdaFunctionRows: Option[DataFrame] = None,
                                     outputExpressionRows: Option[DataFrame] = None,
                                     globalLambdaSuites: Option[DataFrame] = None,
                                     globalOutputExpressionSuites: Option[DataFrame] = None,
                                      ruleSuites: Option[DataFrame] = None
                    ): Option[DataFrame] = None

}
