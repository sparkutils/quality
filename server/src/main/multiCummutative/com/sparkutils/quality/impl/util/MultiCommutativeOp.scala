package com.sparkutils.quality.impl.util

import com.sparkutils.quality.impl.Trigger
import org.apache.spark.sql.catalyst.expressions.{And, Expression, MultiCommutativeOp}

object MultiCommutativeOp {

  lazy val multiOriginalRoot = {
    val o = classOf[MultiCommutativeOp].getDeclaredField("originalRoot")
    o.setAccessible(true)
    o
  }

  def origin(triggers: Seq[Trigger]): Seq[Trigger] =
    triggers.map{
      // 3.4 makes life difficult for this
      case t@ Trigger(m: MultiCommutativeOp, _, _) if m.opCls == classOf[And] =>
        t.copy(expression = multiOriginalRoot.get(m).asInstanceOf[Expression])
      case t => t
    }

}
