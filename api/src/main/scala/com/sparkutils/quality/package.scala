package com.sparkutils

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
  with VersionSpecificSerializingImports  {
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

}

