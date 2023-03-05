package com.sparkutils.quality.impl

import org.apache.spark.sql.InputTypeChecks
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType}
import com.sparkutils.quality.PassedInt

@ExpressionDescription(
  usage = "probability(expr) - Returns the probability from a rule result as a double.",
  examples = """
    Examples:
      > SELECT probability(1000);
       0.01
  """)
case class ProbabilityExpr(child: Expression) extends UnaryExpression with NullIntolerant with InputTypeChecks {
  override def nullSafeEval(res: Any): Any = {
    val full =
      if (res.isInstanceOf[Integer])
        res.asInstanceOf[Integer].toDouble
      else
        res.asInstanceOf[Long].toDouble
    full / PassedInt
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, c => s"((double)($c)) / ${PassedInt}")

  override def nullable: Boolean = false

  override def dataType: DataType = DoubleType

  override def inputDataTypes: Seq[Seq[DataType]] = Seq(Seq(IntegerType, LongType))

  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}
