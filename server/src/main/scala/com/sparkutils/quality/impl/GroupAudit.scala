package com.sparkutils.quality.impl

import com.sparkutils.quality.impl.GroupAudit.typeCheckText
import com.sparkutils.quality.impl.GroupResults.{hasGroupResultType, hasResultType}
import com.sparkutils.quality.impl.util.Compare.equalsIgnoreCaseAndNullability
import com.sparkutils.quality.{RuleSuiteGroupResults, RuleSuiteResult}
import org.apache.spark.sql.ShimUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, NullType, StructType}

import scala.reflect.ClassTag

object GroupAudit {

  def group[T: MergeGroups](rs: Seq[T]): RuleSuiteGroupResults = implicitly[MergeGroups[T]].merge(rs)

  def apply(exprs: Seq[Expression]): GroupAudit = {
    val rsDec = ShimUtils.expressionEncoder(Encoders.ruleSuiteResultExpEnc).resolveAndBind().deserializer
    val rsgEnc = ShimUtils.expressionEncoder(Encoders.ruleSuiteGroupResultsTypedExpEnc).resolveAndBind().objSerializer
    val rsgDec = ShimUtils.expressionEncoder(Encoders.ruleSuiteGroupResultsTypedExpEnc).resolveAndBind().deserializer
    new GroupAudit(Seq(rsgEnc, rsDec, rsgDec) ++ exprs)
  }

  def typeCheckText(typ: Seq[DataType]) = s"GroupAudit supports structures with ruleSuiteGroup: RuleSuiteGroup and " +
    s"ruleSuiteResults: RuleSuiteResult as their first (or as the direct array member), or an array thereof, instead $typ was provided"
}

// at least three children, the actual column, the group expression encoder, the resultsuite expression decoder, the optional processor
trait GroupAuditBase
  extends NonSQLExpression with CodegenFallback {

  def groupFrom[T: MergeGroups: ClassTag](child: Expression, deserializer: Expression, dq: Boolean): InternalRow => RuleSuiteGroupResults = (irow: InternalRow) => {
    val row = child.eval(irow).asInstanceOf[InternalRow]
    val res = deserializer.eval(if (dq) row else row.getStruct(0, 3)).asInstanceOf[T]

    MergeGroups.group[T](Seq(res))
  }

  def groupFromArray[T: MergeGroups: ClassTag](child: Expression, deserializer: Expression, dq: Boolean, s: StructType): InternalRow => RuleSuiteGroupResults = (irow: InternalRow) => {
    val a = child.eval(irow).asInstanceOf[ArrayData]
    val copied = Array.ofDim[T](a.numElements())

    a.foreach(s, (i,e) => {
      val r = e.asInstanceOf[InternalRow]
      copied(i) = deserializer.eval(if (dq) r else r.getStruct(0, 3)).asInstanceOf[T]
    })

    MergeGroups.group[T](copied)
  }

  type IMPL = (DataType, InternalRow => RuleSuiteGroupResults)

  val groupResultEncIndex = 0

  lazy val realChildren = children.drop(3)

  override def checkInputDataTypes(): TypeCheckResult =
    if (!impl.exists(_._1 == NullType))
      TypeCheckResult.TypeCheckSuccess
    else
      TypeCheckResult.TypeCheckFailure(typeCheckText(impl.map(_._1)))

  lazy val impl: Seq[IMPL] =
    realChildren.map(e => e.dataType match {
      case a@ ArrayType(_: StructType, _) if equalsIgnoreCaseAndNullability(a.elementType,
        Encoders.ruleSuiteGroupResultsTypedEnc.catalystRepr) =>
        // existing group results
        withoutPayloadA[RuleSuiteGroupResults](e, a, children(2))
      case a@ ArrayType(_: StructType, _)  if hasGroupResultType(a.elementType.asInstanceOf[StructType]) =>
        // engine, folder, collector
        withPayloadA[RuleSuiteGroupResults](e, a, children(2))
      case a@ ArrayType(_: StructType, _)  if equalsIgnoreCaseAndNullability(a.elementType,
        com.sparkutils.quality.impl.types.ruleSuiteResultType) =>
        // e.g. DQ
        withoutPayloadA[RuleSuiteResult](e, a, children(1))
      case a@ ArrayType(_: StructType, _)  if hasResultType(a.elementType.asInstanceOf[StructType]) =>
        // engine, folder, collector
        withPayloadA[RuleSuiteResult](e, a, children(1))

      // non-arrays
      case d if equalsIgnoreCaseAndNullability(d, Encoders.ruleSuiteGroupResultsTypedEnc.catalystRepr) =>
        // existing group results
        withoutPayload[RuleSuiteGroupResults](e, d, children(2))
      case d: StructType if hasGroupResultType(d) =>
        // engine, folder, collector
        withPayload[RuleSuiteGroupResults](e, d, children(2))
      case d if equalsIgnoreCaseAndNullability(d, com.sparkutils.quality.impl.types.ruleSuiteResultType) =>
        // e.g. DQ
        withoutPayload[RuleSuiteResult](e, d, children(1))
      case d: StructType  if hasResultType(d) =>
        // engine, folder, collector
        withPayload[RuleSuiteResult](e, d, children(1))

      case _ => (NullType, null /* not used */)
    })

  private def withoutPayloadA[T: MergeGroups: ClassTag](child: Expression, a: ArrayType, converter: Expression): IMPL =
    (a, groupFromArray[T](child, converter, true, a.elementType.asInstanceOf[StructType]))

  private def withPayloadA[T: MergeGroups: ClassTag](child: Expression, a: ArrayType, converter: Expression): IMPL =
    (a, groupFromArray[T](child, converter, false, a.elementType.asInstanceOf[StructType]))

  private def withoutPayload[T: MergeGroups: ClassTag](child: Expression, dataType: DataType, converter: Expression): IMPL =
    (dataType, groupFrom[T](child, converter, true))

  private def withPayload[T: MergeGroups: ClassTag](child: Expression, dataType: DataType, converter: Expression): IMPL =
    (dataType, groupFrom[T](child, converter, false))

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val grouped = impl.map(_._2(input))
    val res = MergeGroups.group(grouped)
    children(groupResultEncIndex).eval(InternalRow(res)).asInstanceOf[InternalRow]
  }

  override def dataType: DataType = Encoders.ruleSuiteGroupResultsTypedEnc.catalystRepr

}

// at least three children, the actual column, the group expression encoder, the resultsuite expression decoder, the optional processor
case class GroupAudit(children: Seq[Expression]) extends GroupAuditBase {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)

}
