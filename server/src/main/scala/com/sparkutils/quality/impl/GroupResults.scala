package com.sparkutils.quality.impl

import cats.kernel.{CommutativeGroup, Semigroup}
import com.sparkutils.quality.QualityException.qualityException
import com.sparkutils.quality.impl.GroupResults.{rd, typeCheckText}
import com.sparkutils.quality.{RuleResult, RuleSetResult, RuleSuiteGroupResults, RuleSuiteResult, VersionedId}
import org.apache.spark.sql.ShimUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, NonSQLExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.qualityFunctions.{GroupResultsWithProcess, RefExpression}
import org.apache.spark.sql.types.DataType.equalsIgnoreCaseAndNullability
import org.apache.spark.sql.types.{ArrayType, DataType, NullType, StructField, StructType}

import scala.reflect.ClassTag

trait MergeGroups[T] extends Serializable {
  def merge(items: Seq[T]): RuleSuiteGroupResults
}

object MergeGroups {
  // Used to pull in |+| to deep merge the maps as SemiGroups - https://typelevel.org/cats/typeclasses/semigroup.html#example-usage-merging-maps
  import cats.implicits._

  implicit val ruleResultSemi = new Semigroup[RuleResult] {
    override def combine(x: RuleResult, y: RuleResult): RuleResult =
      // TODO - left wins for overall?
      x
  }

  implicit val ruleSetSemi = new Semigroup[RuleSetResult] {
    override def combine(x: RuleSetResult, y: RuleSetResult): RuleSetResult =
      x.copy(ruleResults = x.ruleResults |+| y.ruleResults)
  }

  implicit val ruleSuiteSemi = new Semigroup[RuleSuiteResult] {
    override def combine(x: RuleSuiteResult, y: RuleSuiteResult): RuleSuiteResult =
      x.copy(ruleSetResults = x.ruleSetResults |+| y.ruleSetResults)
  }

  implicit val rsMergeGroups: MergeGroups[RuleSuiteResult] =
    (items: Seq[RuleSuiteResult]) => {
      items.foldLeft(RuleSuiteGroupResults()) {
        (cur, next) =>
          cur.copy(ruleSuiteResults = cur.ruleSuiteResults |+| Map(next.id -> next))
      }
    }

  implicit val groupMergeGroups: MergeGroups[RuleSuiteGroupResults] =
    (items: Seq[RuleSuiteGroupResults]) => items.foldLeft(RuleSuiteGroupResults()) {
      (cur, next) =>
        cur.copy(ruleSuiteResults = cur.ruleSuiteResults |+| next.ruleSuiteResults)
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
    group.dataType match {
      case ArrayType(_: StructType, _) =>
      case s => qualityException(typeCheckText(s))
    }
    val ref = RefExpression(ArrayType(rd(Seq(group))))

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

  def typeCheckText(typ: DataType) = s"GroupResult supports arrays of structures with ruleSuiteGroup: RuleSuiteGroup and " +
    s"ruleSuiteResults: RuleSuiteResult as their first (or as the direct array member), instead $typ was provided"
}

// at least three children, the actual column, the group expression encoder, the resultsuite expression decoder, the optional processor
trait GroupResultsBase
  extends NonSQLExpression with CodegenFallback {

  def processResult(row: InternalRow, input: ArrayData): Any

  def hasResultTypeF(of: DataType, names: Set[String])(s: StructType): Boolean =
    if (s.fields.exists(
      f => names.contains(f.name) &&
        equalsIgnoreCaseAndNullability(f.dataType, of)))
      true
    else
      false

  val hasResultType = hasResultTypeF(com.sparkutils.quality.impl.types.ruleSuiteResultType, Set("ruleSuiteResults")) _

  // group is used when returned from this expression as the object name, and ruleSuiteResults are the internal (e.g. for dq)
  val hasGroupResultType = hasResultTypeF( Encoders.ruleSuiteGroupResultsTypedEnc.catalystRepr, Set("ruleSuiteGroup", "ruleSuiteResults")) _

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

  override def checkInputDataTypes(): TypeCheckResult =
    if (impl._1 != NullType)
      TypeCheckResult.TypeCheckSuccess
    else
      TypeCheckResult.TypeCheckFailure(typeCheckText(children.head.dataType))

  def processResultType: DataType = ArrayType(rd(children))
  def processResultNullable: Boolean = true

  type IMPL = (DataType, (InternalRow, Any) => Any, Boolean)

  lazy val impl: IMPL =
    children.head.dataType match {
      case a@ ArrayType(_: StructType, _) if equalsIgnoreCaseAndNullability(a.elementType,
        Encoders.ruleSuiteGroupResultsTypedEnc.catalystRepr) =>
        // e.g. DQ
        withoutPayload[RuleSuiteGroupResults](a, children(3))
      case a@ ArrayType(_: StructType, _)  if hasGroupResultType(a.elementType.asInstanceOf[StructType]) =>
        // engine, folder, collector
        withPayload[RuleSuiteGroupResults](a, children(3))
      case a@ ArrayType(_: StructType, _)  if equalsIgnoreCaseAndNullability(a.elementType,
        com.sparkutils.quality.impl.types.ruleSuiteResultType) =>
        // e.g. DQ
        withoutPayload[RuleSuiteResult](a, children(2))
      case a@ ArrayType(_: StructType, _)  if hasResultType(a.elementType.asInstanceOf[StructType]) =>
        // engine, folder, collector
        withPayload[RuleSuiteResult](a, children(2))
      case _ => (NullType, (row, r) => r, true /* not used */)
    }

  private def withoutPayload[T: MergeGroups: ClassTag](a: ArrayType, converter: Expression):  IMPL = {
    (Encoders.ruleSuiteGroupResultsTypedEnc.catalystRepr,
      (row, r) => groupFrom[T](converter, row, true,
        a.elementType.asInstanceOf[StructType], r, identity)._1, false)
  }

  private def withPayload[T: MergeGroups: ClassTag](a: ArrayType, converter: Expression): IMPL = {
    val s = a.elementType.asInstanceOf[StructType]
    val rest = s.fields.map(_.dataType).zipWithIndex.drop(1)

    (resultType, (row, r) => {
      val (gr, res) = groupFrom[T](converter, row, false, s, r, e => {
        val r = rest.map(p => e.get(p._2, p._1))
        if (rest.length == 1)
          r.head // * above
        else
          new GenericInternalRow(r.toArray[Any])
      })
      InternalRow(gr, res)
    }, true)
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
