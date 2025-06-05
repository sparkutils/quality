package com.sparkutils.quality.impl.util

import com.sparkutils.quality._
import com.sparkutils.quality.impl.RuleLogicUtils
import com.sparkutils.shim.expressions.{CreateNamedStruct1, GetStructField3, MapObjects5}
import frameless.TypedEncoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode, ExprValue, JavaCode, QualityExprUtils, VariableValue}
import org.apache.spark.sql.catalyst.expressions.{Alias, BinaryExpression, BoundReference, Expression, If, IsNull, Literal, NamedExpression, Unevaluable, UnsafeArrayData}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, StructField, StructType}

import java.util.concurrent.atomic.AtomicBoolean
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.{GetColumnByOrdinal, UnresolvedAttribute}
import org.apache.spark.sql.{Encoder, ShimUtils, SparkSession}
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.objects.{InitializeJavaBean, Invoke, MapObjects, NewInstance, UnresolvedMapObjects}

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

object Encoding {

  /**
   * Wraps a non Frameless encoder in a TypedEncoder, adjusting paths as needed.
   *
   * This is not intended for general use and is used by the ProcessFunctions.
   *
   * @param outputType
   * @tparam T
   * @return
   */
  def fromNormalEncoder[T: Encoder](outputType: DataType): TypedEncoder[T] = {
    val oexpr = ShimUtils.expressionEncoder(implicitly[Encoder[T]])

    implicit val cltag = oexpr.clsTag

    new TypedEncoder[T] {

      override def nullable: Boolean = true

      override def jvmRepr: DataType = oexpr.deserializer.dataType

      override def catalystRepr: DataType = {
        val se = oexpr.serializer
        if (se.length == 1)
          se.head.dataType
        else
          StructType( // 2.4 only cast
            se.map(n => StructField(n.asInstanceOf[NamedExpression].qualifiedName, n.dataType, n.nullable))
          )
      }

      override def fromCatalyst(path: Expression): Expression = {
        val de = oexpr.deserializer
        val r =
          de match {
            case a: Alias =>
              a.child match {
                case m: UnresolvedMapObjects => a.withNewChildren(Seq( m.copy(child = path) ))
                case a => a.transformUp {
                  case _: GetColumnByOrdinal =>
                    path
                }
              }
            case m: UnresolvedMapObjects => m.copy(child = path)
            case n: NewInstance =>
              val o = outputType.asInstanceOf[StructType].zipWithIndex.map{case (e,i) => e.name -> i }.toMap

              If(IsNull(ForceNullable(path)), Literal(null),
                n.withNewChildren(n.children map {
                  _.transform {
                    case u: UnresolvedAttribute if o.contains(u.name) =>
                      GetStructField3(path, o(u.name))
                  }
                })
              )
            case i: InitializeJavaBean =>
              val o = outputType.asInstanceOf[StructType].zipWithIndex.map{case (e,i) => e.name -> i }.toMap

              If(IsNull(ForceNullable(path)), Literal(null),
                i.copy(setters =
                  i.setters.map{ p =>
                    (p._1, p._2.transform {
                      case u: UnresolvedAttribute if o.contains(u.name) =>
                        GetStructField3(path, o(u.name))
                    })
                  }
                )
              )
            // all single fields from a struct
            case i: Invoke =>
              i.transformUp {
                case _: GetColumnByOrdinal =>
                  path
              }
            case a => a.transformUp {
              case _: GetColumnByOrdinal =>
                path
            }
          }
        r

      }

      // only used by resolveAndBind
      override def toCatalyst(path: Expression): Expression = {
        val se = oexpr.serializer
        if (se.length == 1)
          se.head match {
            case a: Alias =>
              a.child match {
                case m: MapObjects => a.withNewChildren(Seq( m.copy(inputData = path) ))
                case a => a.transformUp {
                  case b: BoundReference => path
                }
              }
            case m: MapObjects => m.copy(inputData = path)
            case a => a.transformUp {
              case b: BoundReference => path
            }
          }
        else {
          val o = outputType.asInstanceOf[StructType]

          val dealiased = se.map {
            case a: Alias =>
              a.name -> a.child.transformUp {
                case b: BoundReference => path
              }
          }.toMap

          CreateNamedStruct1(
            o.fields.map(f => f.name -> dealiased(f.name)).flatMap {
              case (name, e) =>
                Seq[Expression](Literal(name), e)
            }
          )
        }
      }
    }

  }

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
      //case a: VariableValue if ExprUtils.isVariableMutableArray(ctx, a) => None
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