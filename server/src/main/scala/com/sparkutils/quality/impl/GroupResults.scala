package com.sparkutils.quality.impl

import com.sparkutils.quality.impl.GroupResults.rd
import com.sparkutils.quality.{RuleSuiteGroupResults, RuleSuiteResult}
import org.apache.spark.sql.ShimUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, NonSQLExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenFallback}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.qualityFunctions.{GroupResultsWithProcess, RefExpression}
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

  def apply(group: Expression, l: org.apache.spark.sql.catalyst.expressions.LambdaFunction): GroupResultsWithProcess = {
    val rsDec = ShimUtils.expressionEncoder(Encoders.ruleSuiteResultExpEnc).resolveAndBind().deserializer
    val rsgEnc = ShimUtils.expressionEncoder(Encoders.ruleSuiteGroupResultsTypedExpEnc).resolveAndBind().objSerializer
    val ref = RefExpression(ArrayType(rd(Seq(group))))//RefExpressionLazyType(new AtomicReference[DataType](), true)

    GroupResultsWithProcess(Seq(group, rsgEnc, rsDec, ref), l)
  }

  def rd(children: Seq[Expression]) = {
    val a = children.head.dataType.asInstanceOf[ArrayType]
    val s = a.elementType.asInstanceOf[StructType]

    val rs = s.copy(fields = s.fields.drop(1))
    if (rs.fields.length == 1) {
      // folder non-debug and collector do not have extra fields *
      rs.fields.head.dataType
    } else
      rs
  }
}

// at least three children, the actual column, the group expression encoder, the resultsuite expression decoder, the optional processor
trait GroupResultsBase
  extends NonSQLExpression with CodegenFallback {

  def processResult(row: InternalRow, input: ArrayData): Any

  def hasResultType(s: StructType): Boolean =
    if (s.fields.exists(
        f => f.name == "ruleSuiteResults" &&
          equalsIgnoreCaseAndNullability(f.dataType, com.sparkutils.quality.impl.types.ruleSuiteResultType)))
      true
    else
      false

  def groupFrom(row: InternalRow, dq: Boolean, s: StructType, arr: Any, f: InternalRow => Any): (InternalRow, Any) = {
    val a = arr.asInstanceOf[ArrayData]
    val copied = Array.ofDim[RuleSuiteResult](a.numElements())
    val copiedResult = Array.ofDim[Any](a.numElements())

    a.foreach(s, (i,e) => {
      val r = e.asInstanceOf[InternalRow]
      copied(i) = children(2).eval(if (dq) r else r.getStruct(0, 3)).asInstanceOf[RuleSuiteResult]
      copiedResult(i) = f(r)
    })

    (children(1).eval(InternalRow(GroupResults.group(copied))).asInstanceOf[InternalRow],
      processResult(row, new GenericArrayData(copiedResult)))
  }

  def resultType: DataType =
    StructType(Seq(
      StructField("ruleSuiteGroup", Encoders.ruleSuiteGroupResultsTypedEnc.catalystRepr),
      StructField("result",
        processResultType
        , processResultNullable)
    ))

  def processResultType: DataType = ArrayType(rd(children))
  def processResultNullable: Boolean = true

  lazy val impl: (DataType, (InternalRow, Any) => Any) =
    children.head.dataType match {
      case a: ArrayType if equalsIgnoreCaseAndNullability(a.elementType, com.sparkutils.quality.impl.types.ruleSuiteResultType) =>
        // DQ
        (Encoders.ruleSuiteGroupResultsTypedEnc.catalystRepr,
          (row, r) => groupFrom(row, true, a.elementType.asInstanceOf[StructType], r, identity)._1)
      case a: ArrayType if hasResultType(a.elementType.asInstanceOf[StructType]) =>
        // engine, folder, collector
        val s = a.elementType.asInstanceOf[StructType]
        val rest = s.fields.map(_.dataType).zipWithIndex.drop(1)

        (resultType, (row, r) => {
          val (gr, res) = groupFrom(row, false, s, r, e => {
            val r = rest.map(p => e.get(p._2, p._1))
            if (rest.length == 1)
              r.head // * above
            else
              new GenericInternalRow(r.toArray[Any])
          })
          InternalRow(gr, res)
        })
      case _ => (NullType, (row, r) => r)
    }

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any =
    impl._2(input, children.head.eval(input))

  override def dataType: DataType = impl._1

}

// at least three children, the actual column, the group expression encoder, the resultsuite expression decoder, the optional processor
case class GroupResults(children: Seq[Expression], hasProcessor: Boolean = false)
  extends GroupResultsBase {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)

  override def processResult(row: InternalRow, input: ArrayData): Any = input
}
