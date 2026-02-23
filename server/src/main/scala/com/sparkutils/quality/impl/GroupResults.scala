package com.sparkutils.quality.impl

import com.sparkutils.quality.{RuleSuiteGroupResults, RuleSuiteResult, impl}
import com.sparkutils.quality.impl.util.Arrays
import org.apache.spark.sql.ShimUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, UnsafeArrayData}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types.DataType.equalsIgnoreCaseAndNullability
import org.apache.spark.sql.types.{ArrayType, DataType, NullType, StructField, StructType}

object GroupResults {

  // TODO handle merges
  def group(rs: Seq[RuleSuiteResult]): RuleSuiteGroupResults = RuleSuiteGroupResults(rs: _*)
    //rs.foldLeft(RuleSuiteGroupResults())

  def apply(group: Expression): GroupResults = {
    val rsDec = ShimUtils.expressionEncoder(Encoders.ruleSuiteResultExpEnc).resolveAndBind().deserializer
    val rsgEnc = ShimUtils.expressionEncoder(Encoders.ruleSuiteGroupResultsTypedExpEnc).resolveAndBind().objSerializer
    GroupResults(Seq(group, rsgEnc, rsDec))
  }

}

// at least three children, the actual column, the group expression encoder, the resultsuite expression decoder, the optional processor
case class GroupResults(children: Seq[Expression], hasProcessor: Boolean = false) extends Expression with CodegenFallback {

  def withProcessor(lambda: org.apache.spark.sql.catalyst.expressions.LambdaFunction): GroupResults =
    copy(children = children :+ lambda.children.head, hasProcessor = true) // straight up lift, lambda is a convenience here

  def processNoOp(input: InternalRow): Any = input
  def processResult(input: InternalRow): Any = children(3).eval(input)

  lazy val process: InternalRow => Any = if (hasProcessor) processResult else processNoOp

  def hasResultType(s: StructType): Boolean =
    if (s.fields.exists(
        f => f.name == "ruleSuiteResults" &&
          equalsIgnoreCaseAndNullability(f.dataType, com.sparkutils.quality.impl.types.ruleSuiteResultType)))
      true
    else
      false

  def groupFrom(s: DataType, arr: Any, f: InternalRow => Any): (InternalRow, Any) = {
    val a = arr.asInstanceOf[ArrayData]
    val copied = Array.ofDim[RuleSuiteResult](a.numElements())
    val copiedResult = Array.ofDim[Any](a.numElements())

    a.foreach(s, (i,e) => {
      val r = e.asInstanceOf[InternalRow]
      copied(i) = children(2).eval(r).asInstanceOf[RuleSuiteResult]
      copiedResult(i) = f(r)
    })

    (children(1).eval(InternalRow(GroupResults.group(copied))).asInstanceOf[InternalRow],
      process(InternalRow(new GenericArrayData(copiedResult))))
  }

  def resultType(t: DataType): DataType =
    StructType(Seq(
      StructField("ruleSuiteGroup", Encoders.ruleSuiteGroupResultsTypedEnc.catalystRepr),
      StructField("result",
        if (hasProcessor) children(3).dataType else ArrayType(t)
        , if (hasProcessor) children(3).nullable else true)
    ))

  lazy val impl: (DataType, Any => Any) =
    children.head.dataType match {
      case a: ArrayType if equalsIgnoreCaseAndNullability(a.elementType, com.sparkutils.quality.impl.types.ruleSuiteResultType) =>
        // DQ
        (Encoders.ruleSuiteGroupResultsTypedEnc.catalystRepr, groupFrom(a.elementType, _, identity)._1)
      case a: ArrayType if hasResultType(a.elementType.asInstanceOf[StructType]) =>
        // engine, folder, collector
        val s = a.elementType.asInstanceOf[StructType]
        val rest = s.fields.map(_.dataType).zipWithIndex.drop(1)

        (resultType(s), r => {
          val (gr, res) = groupFrom(s, r, e => {
            new GenericInternalRow(rest.map(p => e.get(p._2, p._1)).toArray[Any])
          })
          InternalRow(gr, res)
        })
      case _ => (NullType, identity)
    }

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any =
    impl._2(children(0).eval(input))

  override def dataType: DataType = impl._1

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(newChildren)
}
