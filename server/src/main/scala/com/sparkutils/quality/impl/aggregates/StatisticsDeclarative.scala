package com.sparkutils.quality.impl.aggregates

import com.sparkutils.quality.ResultStatisticsProvider.ResultStatisticOps
import com.sparkutils.quality.{RuleSuiteGroupResults, RuleSuiteGroupStatistics, RuleSuiteResult, RuleSuiteStatistics}
import com.sparkutils.quality.impl.util.Compare
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BinaryExpression, Expression, Literal, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.trees.{BinaryLike, UnaryLike}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types.{DataType, ObjectType}
import org.apache.spark.sql.catalyst.dsl.expressions._

case class ProcessStatistics(children: Seq[Expression]) extends Expression with CodegenFallback {

  lazy val Seq(grp, col, groupSer, groupDer, ruleDer) = children

  override def eval(input: InternalRow): Any = {
    val lgrp = groupDer.eval(grp.eval(input).asInstanceOf[InternalRow]).asInstanceOf[RuleSuiteGroupStatistics]
    val rgrp = ruleDer.eval(col.eval(input).asInstanceOf[InternalRow]).asInstanceOf[RuleSuiteResult]
    val r = lgrp.process(rgrp)
    groupSer.eval(InternalRow(r))
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)

  override def dataType: DataType = com.sparkutils.quality.impl.Encoders.ruleSuiteGroupStatisticsTypedExpEnc.schema

  override def nullable: Boolean = false
}

case class MergeStatistics(children: Seq[Expression]) extends Expression with CodegenFallback {

  lazy val Seq(left, right, groupSer, groupDer) = children

  override def eval(input: InternalRow): Any = {
    val lgrp = groupDer.eval(left.eval(input).asInstanceOf[InternalRow]).asInstanceOf[RuleSuiteGroupStatistics]
    val rgrp = groupDer.eval(right.eval(input).asInstanceOf[InternalRow]).asInstanceOf[RuleSuiteGroupStatistics]
    val r = lgrp.combine(rgrp)
    groupSer.eval(InternalRow(r))
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)

  override def dataType: DataType = com.sparkutils.quality.impl.Encoders.ruleSuiteGroupStatisticsTypedExpEnc.schema

  override def nullable: Boolean = false
}

/**
 *
 * @param left the column
 * @param right the objSerializer
 */
case class StatisticsDeclarative(children: Seq[Expression]) extends DeclarativeAggregate {

  lazy val Seq(col, groupSer, groupDer, ruleDer) = children

  override def checkInputDataTypes(): TypeCheckResult =
    if (Compare.equalsIgnoreCaseAndNullability(col.dataType, com.sparkutils.quality.impl.types.ruleSuiteResultType))
      TypeCheckResult.TypeCheckSuccess
    else
      TypeCheckResult.TypeCheckFailure(s"Statistics accepts RuleSuiteResult columns but was provided ${col.dataType}")

  val sumDataType = com.sparkutils.quality.impl.Encoders.ruleSuiteGroupStatisticsTypedExpEnc.schema
  lazy val sum = AttributeReference("sum", sumDataType)()

  override val initialValues: Seq[Expression] = Seq(new Literal(
    groupSer.eval(InternalRow(RuleSuiteGroupStatistics())),
    sumDataType))
  override val updateExpressions: Seq[Expression] = Seq(
    ProcessStatistics(Seq(sum, col, groupSer, groupDer, ruleDer))
  )
  override val mergeExpressions: Seq[Expression] = Seq(
    MergeStatistics(Seq(sum.left, sum.right, groupSer, groupDer))
  )
  override val evaluateExpression: Expression = sum

  override def aggBufferAttributes: Seq[AttributeReference] = Seq(sum)

  override def nullable: Boolean = false

  override def dataType: DataType = sumDataType

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)
}
