package com.sparkutils.quality.utils

import com.sparkutils.quality._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, Unevaluable, UnsafeArrayData}
import org.apache.spark.sql.types.{BooleanType, DataType, StructType}
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.sql.catalyst.util.ArrayData

import scala.annotation.tailrec

/**
 * Same as unevaluable but the queryplan runs
 * @param children
 */
case class PassThrough(children: Seq[Expression]) extends Expression with CodegenFallback {
  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = Literal(true).eval(input)

  override def dataType: DataType = BooleanType

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(children = newChildren)
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

trait LookupIdFunctions {

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
   * Should not be used by users but currently (0.7.0) only forces re-evaluation of the quality.lambdaHandlers configuration rather than caching once
   */
  protected[quality] def setTesting() = {
    testingFlag.set(true)
  }

  def testing = testingFlag.get
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
    left.asInstanceOf[T].compareTo( right.asInstanceOf[T])

  /**
   * Forwards to compare, allows for compareToOrdering(ordering) syntax with internal casts
   * @param left
   * @param right
   * @tparam T
   * @return
   */
  def compareToOrdering[T](ordering: Ordering[T])(left: Any, right: Any): Int =
    ordering.compare(left.asInstanceOf[T], right.asInstanceOf[T])

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
  def mapArray(array: ArrayData, dataType: DataType, f: Any => Any): Array[Any] =
    array match {
      case _: UnsafeArrayData =>
        val res = Array.ofDim[Any](array.numElements())
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