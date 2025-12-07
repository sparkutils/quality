package com.sparkutils.quality.impl.util

import com.sparkutils.quality._
import com.sparkutils.quality.impl.{RuleLogicUtils, ThreeOnlyNonFoldable}
import com.sparkutils.shim.expressions.{CreateNamedStruct1, GetStructField3, MapObjects5}
import frameless.TypedEncoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode, ExprValue, JavaCode, QualityExprUtils, VariableValue}
import org.apache.spark.sql.catalyst.expressions.{Alias, BinaryExpression, BoundReference, Expression, If, IsNull, Literal, NamedExpression, UnaryExpression, Unevaluable, UnsafeArrayData}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, MapType, StructField, StructType}

import java.util.concurrent.atomic.AtomicBoolean
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.{GetColumnByOrdinal, UnresolvedAttribute}
import org.apache.spark.sql.{Encoder, ShimUtils, SparkSession}
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.objects.{InitializeJavaBean, Invoke, MapObjects, NewInstance, UnresolvedMapObjects}

import scala.annotation.{elidable, tailrec}
import scala.reflect.ClassTag

trait PassThrough extends Expression {
  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = Literal(true).eval(input)

  override def dataType: DataType = BooleanType
}

/**
 * Same as unevaluable but the queryplan runs.  This version should only be used for eval (compileEvals = false) of
 * rules / triggers and for any output expressions, it may take part in SubExprEvaluationRuntime
 * @param children
 */
case class PassThroughEvalOnly(children: Seq[Expression]) extends PassThrough with Unevaluable {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(children = newChildren)

}

/**
 * Same as unevaluable but the queryplan runs.  This version requires compileEvals = true (rules are independent and
 * will not use Subexpression Elimination at eval time) and as such cannot be used with SubExprEvaluationRuntime
 * @param children
 */
case class PassThroughCompileEvals(child: Expression) extends UnaryExpression with PassThrough with CodegenFallback {

  protected def withNewChildInternal(newChild: Expression): Expression = copy(newChild)

  override def nullable: Boolean = child.nullable

  override def eval(input: InternalRow): Any = child.eval(input)

  override def dataType: DataType = child.dataType
}

/**
 * Should not be used in queryplanning  TODO verify if this still needs to be unevaluable, it did under spark 2.4
 * @param rules should be hidden from plans
 */
case class NonPassThrough(rule: Expression) extends UnaryExpression with ThreeOnlyNonFoldable with Unevaluable {

  override def nullable: Boolean = true

  override def dataType: DataType = BooleanType

  override def child: Expression = Literal(true)

  protected def withNewChildInternal(newChild: Expression): Expression = copy(newChild)

}

object LookupIdFunctions {

  def namesFromSchema(schema: StructType): Set[String] = {

    def withParent(name: String, parent: String) =
      if (parent.isEmpty)
        name
      else
        parent + "." + name

    def accumulate(set: Set[String], schema: StructType, parent: String): Set[String] =
      schema.foldLeft(set) {
        (s, field) =>
          val name = withParent(field.name, parent)
          field.dataType match {
            case struct: StructType =>
              accumulate(s + name, struct, name)
            case _ => s + name
          }
      }

    accumulate(Set.empty, schema, "")
  }

  /**
   * Use this function to identify which maps / blooms etc. are used by a given rulesuite
   * collects all rules that are using lookup functions but without constant expressions and the list of lookups that are constants.
   *
   */
  def identifyLookups(ruleSuite: RuleSuite): LookupResults = {
    val olambdaResults =
      ruleSuite.lambdaFunctions.flatMap{r =>
        val exp = RuleLogicUtils.expr(r.rule)
        LookupIdFunctionImpl.identifyLookups(exp).map((_,r))
      }.foldLeft(ExpressionLookupResults[Id](Map.empty, Set.empty)) {
        (acc, res) =>
          val r = acc.copy( lookupConstants = acc.lookupConstants + (res._2.id -> res._1.constants))
          if (res._1.hasExpressionLookups)
            r.copy(lookupExpressions = r.lookupExpressions + res._2.id)
          else
            r
      }
    val lambdaResults = olambdaResults.copy(lookupConstants = olambdaResults.lookupConstants.filter(_._2.nonEmpty))

    LookupResults(ruleSuite, ExpressionLookupResults(Map.empty, Set.empty), lambdaResults)
  }
}

case class TSLocal[T](val initialValue: () => T) extends Serializable {
  @volatile @transient private var threadLocal: ThreadLocal[T] = _
  def get(): T = {
    if (threadLocal eq null) {
      val init = initialValue
      this.synchronized {

        threadLocal = new ThreadLocal[T] {
          override def initialValue(): T = init()
        }

      }
    }
    threadLocal.get()
  }
}

case class TransientHolder[T](val initialise: () => T) extends Serializable {
  @volatile @transient private var it: T = _
  def get(): T = {
    if (it == null) {
      this.synchronized {

        it = initialise()

      }
    }
    it
  }
  def reset: Unit ={
    this.synchronized {
      it = null.asInstanceOf[T]
    }
  }
}

object Arrays {
  /**
   * UnsafeArrayData doesn't allow calling .array, foreach when needed and for others use array
   *
   * @param array
   * @param dataType
   * @param f
   * @return
   */
  def mapArray[T: ClassTag](array: ArrayData, dataType: DataType, f: Any => T): Array[T] =
    array match {
      case _: UnsafeArrayData =>
        val res = Array.ofDim[T](array.numElements())
        array.foreach(dataType, (i, v) => res.update(i, f(v)))
        res
      case _ => array.array.map(f)
    }

  /**
   * gets an array out of UnsafeArrayData or others
   * @param array
   * @param dataType
   * @return
   */
  def toArray(array: ArrayData, dataType: DataType): Array[Any] =
    array match {
      case _: UnsafeArrayData =>
        mapArray(array, dataType, identity)
      case _ => array.array
    }

}

/**
 * Frameless sets path in foldable encoders to nullable == false, but it really is nullable
 * Spark then just accesses the struct which is null.  This forces codegen only
 */
case class ForceNullable(child: Expression) extends Expression {

  val children = Seq(child)

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = child.eval(input)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val c = child.genCode(ctx)
    val typ = JavaCode.javaType(dataType)
    val boxed = JavaCode.boxedType(dataType)
    ev.copy(code =
      code"""
            ${c.code}
            boolean ${ev.isNull} = true;
            $typ ${ev.value} = null;
            if (${c.value} != null) {
              ${ev.isNull} = false;
              ${ev.value} = ($boxed) ${c.value};
            }
            """)
  }


  override def dataType: DataType = child.dataType

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(child = newChildren.head)
}


/**
 * wrap subexprs so we can correctly identify the subquery post bindreferences
 * @param children
 */
case class SubQueryWrapper(children: Seq[Expression]) extends Expression {

  override def nullable: Boolean = children.head.nullable
  override def foldable: Boolean = false
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = children.head.genCode(ctx)
    expr
  }

  override def eval(input: InternalRow): Any = children.head.eval(input)

  override def dataType: DataType = children.head.dataType

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(children = newChildren)
}

object SubQueryWrapper {
  def hasASubQuery(expr: Expression): Boolean =
    (expr.collectFirst {
      case s: SubQueryWrapper => s
    }.isDefined)
}

object Params {

  def stripBrackets(v: VariableValue): (String, String) = {
    val openb = v.toString().indexOf("[")
    if (openb == -1)
      (v.variableName, "")
    else
      (v.variableName.dropRight(v.length - openb), v.variableName.drop(openb))
  }

  def formatParams(ctx: CodegenContext, a: Seq[ExprValue], callsKeepArrays: Boolean = false): (String, String) = {
    // filter out any top level arrays, the input is a set, so params need the same order
    val ordered = a.flatMap {
      case a: VariableValue => Some(a)
      case _ => None
    }

    (ordered.map { v =>
      val (stripped, arrayInName) = stripBrackets(v)

      val (typ, array) =
        if (v.javaType.isArray)
          (s"${v.javaType.getComponentType.getName}", "[]")
        else if (v.javaType.isPrimitive)
          (v.javaType.toString, arrayInName.replaceAll("[^\\[\\]]",""))
        else
          (v.javaType.getName, arrayInName.replaceAll("[^\\[\\]]",""))

      s"$typ$array $stripped"
    }.mkString(", ")
      , ordered.map(v =>
        if (v.javaType.isArray && callsKeepArrays)
          v.variableName
        else
          stripBrackets(v)._1
      ).mkString(", "))
  }
}


/**
 * Wraps an expression on the right, using an input field on the left, forcing resolution of processor expressions
 * not using input fields.
 *
 * @param left completely ignored and only present to force correct resolution via predicate helper
 * @param right actual code to use, typically a processor
 */
case class InputWrapper(left: Expression, right: Expression) extends BinaryExpression {

  override def nullable: Boolean = right.nullable

  override def eval(input: InternalRow): Any = right.eval(input)

  protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = InputWrapper(newLeft, newRight)

  override def dataType: DataType = right.dataType

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    right.genCode(ctx)
}
/*
object TypeUtils {

  private def mapType(l: MapType, r: MapType) =
    equivalent(l.keyType, r.keyType) && equivalent(l.valueType, r.valueType)

  /**
   * Compares struct fields without using nullability
   * @param left
   * @param right
   * @return
   */
  @tailrec
  def equivalent(left: DataType, right: DataType): Boolean =
    (left, right) match {
      case (l: StructType, r: StructType) if l.fields.length == r.fields.length =>
        l.copy(fields = l.fields.map(f => f.copy(nullable = true))) ==
          r.copy(fields = r.fields.map(f => f.copy(nullable = true)))
      case (_: StructType, _: StructType) =>
        false
      case (l: ArrayType, r: ArrayType) =>
        equivalent(l.elementType, r.elementType)
      case (l: MapType, r: MapType) =>
        mapType(l, r)
      case _ => left == right
    }
} */