package com.sparkutils.quality.impl

import org.apache.spark.sql.catalyst.InternalRow

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.qualityFunctions.InputTypeChecks
import org.apache.spark.sql.types._

object RuleSuiteResultDetails {
  def getDetails(input: scala.Any): InternalRow = {
    // should actually be a row
    val ir = input.asInstanceOf[InternalRow]
    InternalRow(ir.getLong(0), ir.getMap(2))
  }
}

/**
  * Consumes a RuleSuiteResult and returns RuleSuiteDetails
  */
case class RuleSuiteResultDetails(child: Expression) extends UnaryExpression with NonSQLExpression with InputTypeChecks {

  override def inputDataTypes: Seq[Seq[DataType]] = Seq(Seq(com.sparkutils.quality.ruleSuiteResultType))

  override protected def nullSafeEval(input : scala.Any) : scala.Any = RuleSuiteResultDetails.getDetails(input)

  def dataType: DataType = com.sparkutils.quality.ruleSuiteDetailsResultType

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, c => s"com.sparkutils.quality.impl.RuleSuiteResultDetails.getDetails($c)")

  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}
