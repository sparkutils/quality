package com.sparkutils.quality.impl

import com.sparkutils.quality.impl.util.{NonPassThrough, PassThroughCompileEvals}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionProxy}

object GetRealChildren {

  /**
   * unpack correct children to eval/compile against.
   * @param children
   * @return
   */
  def getRealChildren(children: Seq[Expression]): Seq[Expression] =
    children.map {
      case r @ NonPassThrough(_) => r.rule
      case PassThroughCompileEvals(child) => child
      case e: ExpressionProxy if e.child.isInstanceOf[PassThroughCompileEvals] => e.child.children.head
      case child => child
    }

}
