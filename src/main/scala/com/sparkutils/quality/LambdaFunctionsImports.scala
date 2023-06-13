package com.sparkutils.quality

import org.apache.spark.sql.qualityFunctions.LambdaFunctions

trait LambdaFunctionsImports {
  /**
   * This function is for use with directly loading lambda functions to use within normal spark apps.
   *
   * Please be aware loading rules via ruleRunner, addOverallResultsAndDetails or addDataQuality will also load any
   * registered rules applicable for that ruleSuite and may override other registered rules.  You should not use this directly
   * and RuleSuites for that reason (otherwise your results will not be reproducible).
   *
   * @param functions
   */
  def registerLambdaFunctions(functions: Seq[LambdaFunction]): Unit =
    LambdaFunctions.registerLambdaFunctions(functions)

  type IdTriple = (Id, Id, Id)
}
