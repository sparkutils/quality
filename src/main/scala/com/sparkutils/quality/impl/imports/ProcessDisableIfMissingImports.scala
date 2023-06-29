package com.sparkutils.quality.impl.imports

import com.sparkutils.quality.RuleSuite
import com.sparkutils.quality.impl.ProcessDisableIfMissing
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.StructType

trait ProcessDisableIfMissingImports {
  /**
   * Processes a given RuleSuite to replace any coalesceIfMissingAttributes.  This may be called before validate / docs but
   * *must* be called *before* adding the expression to a dataframe.
   *
   * @param ruleSuite
   * @param schema The names to validate against, if empty no attempt to process coalesceIfAttributeMissing will be made
   * @return
   */
  def processIfAttributeMissing(ruleSuite: RuleSuite, schema: StructType = StructType(Seq())) =
    ProcessDisableIfMissing.processIfAttributeMissing(ruleSuite, schema)

  def processCoalesceIfAttributeMissing(expression: Expression, names: Set[String]): Expression =
    ProcessDisableIfMissing.processCoalesceIfAttributeMissing(expression, names)
}
