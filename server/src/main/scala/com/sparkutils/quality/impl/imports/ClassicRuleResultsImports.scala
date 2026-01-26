package com.sparkutils.quality.impl.imports

import com.sparkutils.quality.impl.imports.RuleResultsImports.{DisabledRuleInt, FailedInt, PassedInt, SoftFailedInt, IgnoredRuleInt}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.unsafe.types.UTF8String

object ClassicRuleResultsImports {

  def strLit(str: String) =
    UTF8String.fromString(str)

  val strLitA = (str: Any) =>
    UTF8String.fromString(str.asInstanceOf[String])

  val SoftFailedExpr = Literal(SoftFailedInt, IntegerType)
  val DisabledRuleExpr = Literal(DisabledRuleInt, IntegerType)
  val IgnoredRuleExpr = Literal(IgnoredRuleInt, IntegerType)
  val PassedExpr = Literal(PassedInt, IntegerType)
  val FailedExpr = Literal(FailedInt, IntegerType)

}
