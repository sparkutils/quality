package com.sparkutils.quality.impl.util

import com.sparkutils.quality._
import com.sparkutils.quality.impl.RuleLogicUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode, JavaCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, UnaryExpression, Unevaluable, UnsafeArrayData}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{BooleanType, DataType, StructType}

import java.util.concurrent.atomic.AtomicBoolean
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper

import scala.reflect.ClassTag

object DebugTime extends Logging {

  def debugTime[T](what: String, log: (Long, String)=>Unit = (i, what) => {logDebug(s"----> ${i}ms for $what")} )( thunk: => T): T = {
    val start = System.currentTimeMillis
    try {
      thunk
    } finally {
      val stop = System.currentTimeMillis

      log(stop - start, what)
    }
  }

}

trait PassThrough extends Expression {
  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = Literal(true).eval(input)

  override def dataType: DataType = BooleanType

  // TODO #21 - migrate to withNewChildren when 2.4 is dropped
  def withNewChilds(newChildren: IndexedSeq[Expression]): Expression
}

/**
 * Same as unevaluable but the queryplan runs.  This version requires compileEvals = true (rules are independent and
 * will not use Subexpression Elimination at eval time) and as such cannot be used with SubExprEvaluationRuntime
 * @param children
 */
case class PassThroughCompileEvals(children: Seq[Expression]) extends PassThrough with CodegenFallback {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(children = newChildren)

  override def withNewChilds(newChildren: IndexedSeq[Expression]): Expression = copy(children = newChildren)
}

/**
 * Same as unevaluable but the queryplan runs.  This version should only be used for eval (compileEvals = false) of
 * rules / triggers and for any output expressions, it may take part in SubExprEvaluationRuntime
 * @param children
 */
case class PassThroughEvalOnly(children: Seq[Expression]) extends PassThrough {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(children = newChildren)

  protected def doGenCode(ctx: org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext, ev: org.apache.spark.sql.catalyst.expressions.codegen.ExprCode): org.apache.spark.sql.catalyst.expressions.codegen.ExprCode = ???

  override def withNewChilds(newChildren: IndexedSeq[Expression]): Expression = copy(children = newChildren)
}

/**
 * Should not be used in queryplanning
 * @param rules should be hidden from plans
 */
case class NonPassThrough(rules: Seq[Expression]) extends Unevaluable {
  override def nullable: Boolean = true

  override def dataType: DataType = BooleanType

  override def children: Seq[Expression] = Seq(Literal(true))

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = this
}

sealed trait LookupType {
  val name: String
}

case class MapLookupType(name: String) extends LookupType
case class BloomLookupType(name: String) extends LookupType

/**
 * Represents the results of lookups.  RuleRows will have empty expressions
 *
 * @param ruleSuite
 * @param ruleResults
 * @param lambdaResults it's not always possible to toString against an expression tree
 */
case class LookupResults(ruleSuite: RuleSuite, ruleResults: ExpressionLookupResults[RuleRow], lambdaResults: ExpressionLookupResults[Id])

case class ExpressionLookupResults[A](lookupConstants: Map[A, Set[LookupType]], lookupExpressions: Set[A])

case class ExpressionLookupResult(constants: Set[LookupType], hasExpressionLookups: Boolean)


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

/**
 * Signifies that testing is being done, it should be ignored by users.
 */
object Testing {
  // horrible hack for testing, but at least attempt to make it performant
  private val testingFlag = new AtomicBoolean(false)

  /**
   * Should not be used by users but currently (0.0.2) only forces re-evaluation of the quality.lambdaHandlers configuration rather than caching once.
   */
  protected[quality] def setTesting() = {
    testingFlag.set(true)
  }

  def testing = testingFlag.get

  /**
   * Should not be called by users of the library and is provided for testing support only
   * @param thunk
   */
  def test(thunk: => Unit): Unit = try {
    setTesting()
    thunk
  } finally {
    testingFlag.set(false)
  }
}

object Comparison {

  /**
   * Forwards to compareTo, allows for compareTo[Type] syntax with internal casts
   * @param left
   * @param right
   * @tparam T
   * @return
   */
  def compareTo[T <: Comparable[T]](left: Any, right: Any): Int =
    if (left == null && right != null)
      -100
    else
      if (right == null && left != null)
        100
      else
        left.asInstanceOf[T].compareTo( right.asInstanceOf[T])

  /**
   * Forwards to compare, allows for compareToOrdering(ordering) syntax with internal casts
   * @param left
   * @param right
   * @tparam T
   * @return
   */
  def compareToOrdering[T](ordering: Ordering[T])(left: Any, right: Any): Int =
    if (left == null && right != null)
      -100
    else
      if (right == null && left != null)
        100
      else
        ordering.compare(left.asInstanceOf[T], right.asInstanceOf[T])

}

object Optional {
  def toOptional[T](option: Option[T]): java.util.Optional[T] =
    if (option.isEmpty)
      java.util.Optional.empty()
    else
      java.util.Optional.of(option.get)
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
 * With the introduction of the 4 runtime folder needs different
 * resolved behaviour on lazytyperef, as such these move here
 * from TestUtils
 */
object SparkVersions {

  lazy val sparkFullVersion = {
    val pos = classOf[Expression].getPackage.getSpecificationVersion
    if ((pos eq null) || pos == "0.0") // DBR is always null, Fabric 0.0
      SparkSession.active.version
    else
      pos
  }

  lazy val sparkVersion = sparkFullVersion.split('.').take(2).mkString(".")

  lazy val sparkMajorVersion = sparkFullVersion.split('.').head
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