package com.sparkutils.quality.impl

import com.sparkutils.quality.impl.GroupResults.rd
import com.sparkutils.quality.{RuleSuiteGroupResults, RuleSuiteResult}
import org.apache.spark.sql.ShimUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, NonSQLExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.qualityFunctions.{GroupResultsWithProcess, RefExpression}
import org.apache.spark.sql.types.DataType.equalsIgnoreCaseAndNullability
import org.apache.spark.sql.types.{ArrayType, DataType, NullType, StructField, StructType}

import scala.reflect.ClassTag

trait MergeGroups[T] {
  def merge(items: Seq[T]): RuleSuiteGroupResults
}
// TODO handle merges

object MergeGroups {
  implicit val rsMergeGroups: MergeGroups[RuleSuiteResult] = new MergeGroups[RuleSuiteResult] {
    override def merge(items: Seq[RuleSuiteResult]): RuleSuiteGroupResults = RuleSuiteGroupResults(items: _*)
  }
  implicit val groupMergeGroups: MergeGroups[RuleSuiteGroupResults] = new MergeGroups[RuleSuiteGroupResults] {
    override def merge(items: Seq[RuleSuiteGroupResults]): RuleSuiteGroupResults =
      items.foldLeft(RuleSuiteGroupResults()) {
        (cur, next) =>
          cur.copy(ruleSuiteResults = cur.ruleSuiteResults ++ next.ruleSuiteResults)
      }
  }
}

object GroupResults {

  def group[T: MergeGroups](rs: Seq[T]): RuleSuiteGroupResults = implicitly[MergeGroups[T]].merge(rs)

  def apply(group: Expression): GroupResults = {
    val rsDec = ShimUtils.expressionEncoder(Encoders.ruleSuiteResultExpEnc).resolveAndBind().deserializer
    val rsgEnc = ShimUtils.expressionEncoder(Encoders.ruleSuiteGroupResultsTypedExpEnc).resolveAndBind().objSerializer
    val rsgDec = ShimUtils.expressionEncoder(Encoders.ruleSuiteGroupResultsTypedExpEnc).resolveAndBind().deserializer
    GroupResults(Seq(group, rsgEnc, rsDec, rsgDec))
  }

  def apply(group: Expression, l: org.apache.spark.sql.catalyst.expressions.LambdaFunction): GroupResultsWithProcess = {
    val rsDec = ShimUtils.expressionEncoder(Encoders.ruleSuiteResultExpEnc).resolveAndBind().deserializer
    val rsgEnc = ShimUtils.expressionEncoder(Encoders.ruleSuiteGroupResultsTypedExpEnc).resolveAndBind().objSerializer
    val rsgDec = ShimUtils.expressionEncoder(Encoders.ruleSuiteGroupResultsTypedExpEnc).resolveAndBind().deserializer
    val ref = RefExpression(ArrayType(rd(Seq(group))))//RefExpressionLazyType(new AtomicReference[DataType](), true)

    GroupResultsWithProcess(Seq(group, rsgEnc, rsDec, rsgDec, ref), l)
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

  def groupFrom[T: MergeGroups: ClassTag](deserializer: Expression, row: InternalRow, dq: Boolean, s: StructType, arr: Any, f: InternalRow => Any): (InternalRow, Any) = {
    val a = arr.asInstanceOf[ArrayData]
    val copied = Array.ofDim[T](a.numElements())
    val copiedResult = Array.ofDim[Any](a.numElements())

    a.foreach(s, (i,e) => {
      val r = e.asInstanceOf[InternalRow]
      copied(i) = deserializer.eval(if (dq) r else r.getStruct(0, 3)).asInstanceOf[T]
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
          (row, r) => groupFrom[RuleSuiteResult](children(2), row, true, a.elementType.asInstanceOf[StructType], r, identity)._1)
      case a: ArrayType if hasResultType(a.elementType.asInstanceOf[StructType]) =>
        // engine, folder, collector
        val s = a.elementType.asInstanceOf[StructType]
        val rest = s.fields.map(_.dataType).zipWithIndex.drop(1)

        (resultType, (row, r) => {
          val (gr, res) = groupFrom[RuleSuiteResult](children(2), row, false, s, r, e => {
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
