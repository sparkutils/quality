package com.sparkutils.quality.impl.util

import com.sparkutils.quality.impl.extension.ZeroCodeGenWrap
import com.sparkutils.quality.impl.{RuleFolderRunner, RuleFolderRunnerBase, RuleFolderRunnerEval, Runner}
import org.apache.spark.sql.ShimUtils.column
import org.apache.spark.sql.{ClassicQualitySparkUtils, Column, DataFrame}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionProxy}

object GetRealChildren {

  /**
   * unpack correct children to eval/compile against.
   * @param children
   * @return
   */
  def getRealChildren(children: Seq[Expression]): Seq[Expression] =
    children.map {
      case r: NonPassThrough => r.rule
      case PassThroughCompileEvals(child) => child
      case e: ExpressionProxy if e.child.isInstanceOf[PassThroughCompileEvals] => e.child.children.head
      case child => child
    }

  def resolveWithOverride(resolveWith: Option[DataFrame], runner: Runner): Column = {
    column(
      ClassicQualitySparkUtils.resolveWithOverride(resolveWith).map { df =>
        val resolved = ClassicQualitySparkUtils.resolveExpression(df, runner)

        resolved.withNewChildren(resolved.children.map {
          // replace the expr
          case PassThroughCompileEvals(child) => NonPassThrough(child)
          case child => child
        })
      } getOrElse ZeroCodeGenWrap.wrap(runner)
    )
  }
}
