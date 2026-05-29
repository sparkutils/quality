package com.sparkutils

import com.sparkutils.quality.impl.VariableProcessIfMissing
import com.sparkutils.quality.impl.constants.RuleProcessingConstants
import com.sparkutils.quality.impl.imports._
import com.sparkutils.quality.impl.mapLookup.MapLookupImportsShared
import com.sparkutils.quality.impl.util.{AddDataFunctionsImports, SerializingImports, VersionSpecificSerializingImports}
import com.sparkutils.quality.impl.views.ViewLoading
import org.apache.spark.sql.internal.SQLConf

/**
 * Provides an easy import point for the library.
 */
package object quality extends RuleRunnerImports with Serializable with MapLookupImportsShared with SerializingImports
  with AddDataFunctionsImports with LambdaFunctionsImports with RuleEngineRunnerImports
  with RuleFolderRunnerImports with ViewLoading with ExpressionRunnerImports
  with VersionSpecificSerializingImports with VariableProcessIfMissing
  with CollectRunnerImports with RuleProcessingConstants {
  // NB it must inherit Serializable due to the nested types and sparks serialization

  /**
   * first attempts to get the system env, then system java property then sqlconf
   * @param name
   * @return
   */
  def getConfig(name: String, default: String = "") = try {
    val res = System.getenv(name)
    if (res ne null)
      res
    else {
      val sp = System.getProperty(name)
      if (sp ne null)
        sp
      else
        SQLConf.get.getConfString(name, default)
    }
  } catch {
    case _: Throwable => default
  }

  /**
   * Simplified registerQualityFunctions, use classicFunction import when the other features are needed.
   *
   * Must be called before using any functions like Passed, Failed or Probability(X) when using classic, a no-op when
   * using connect with the SparkSessionExtension
   */
  def registerQualityFunctions(): Unit = RegistrationFunction.registerQualityFunctions()
}

