package com.sparkutils.quality.impl.util

import com.sparkutils.quality._
import com.sparkutils.quality.impl.{RuleLogicUtils, ThreeOnlyNonFoldable}
import net.jpountz.lz4.{LZ4BlockInputStream, LZ4BlockOutputStream, LZ4Factory}
import net.jpountz.xxhash.XXHashFactory
import org.apache.spark.SparkConf
import org.apache.spark.internal.config.IO_COMPRESSION_LZ4_BLOCKSIZE
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, Literal, UnaryExpression, Unevaluable, UnsafeArrayData}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.types.{BooleanType, DataType, StructType}

import java.io.{InputStream, OutputStream}
import scala.reflect.ClassTag

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

}

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

object ClassicLookupIdFunctions {

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

/**
 *
 * @param paramsDef drop in for function lists
 * @param paramsCall drop in for function calls
 * @param arity the arity of the parameters, abstract function only goes to 22, 255 are available
 * @param pushToTop any outer context information (spark 3.1 and higher)
 * @param params pairs of variable name to java type used for declaration and the class type for boxing
 */
case class ParameterInformation(paramsDef: String, paramsCall: String, arity: Int,
                                params: Seq[(String, String, Class[_], Boolean)], pushToTop: String = "",
                                outerCallParams: String = "",
                                // split expressions pairs
                                nonCombinedParams: Seq[(String, String, Class[_], Boolean)] = Seq.empty
                               ) {

  val useArity = if (arity > 22) 1 else arity

  /**
   * When arity is over 22 we still need a type, so the type becomes an array we unpack..., boxing is unavoidable
   *
   * Arity of 0 implies no actual input information is needed, e.g. a rule is hardcode / folded to a constant
   *
   * @return
   */
  def aritySafeApplyType(prefix: String): String =
    s"$prefix$useArity<InternalRow${if (arity > 0) "," else ""}" +
      (
        if (arity <= 22)
          params.map { p => "Object"
/*            if (p._3.isPrimitive)
              CodeGenerator.boxedType(p._3.getSimpleName)
            else
              p._1*/
          }.mkString(",")
        else
          "Object"
        ) + ">"

  def aritySafeParamDef: String =
    if (arity <= 22)
      params.map{ p=>
        s"Object ${p._2}_ppp" // only object will compile, janino no generics
      }.distinct.mkString(",")
    else
      "Object input_ppp"

  def addAritySafeParamDecl(ctx: CodegenContext): Unit =
    params.map { p =>
      val (arrayExtraDecl, arrayExtraDim) =
        if (p._4)
          (p._2, "[]") // s" = new ${p._3.componentType().getName}[1][]
        else
          (p._2, "")

      /*s"private ${p._1}$arrayExtraDim ${p._2};" */// TODO dim handling?
      ctx.addMutableState(CodeGenerator.typeName(p._3)+arrayExtraDim, p._2, forceInline = true, useFreshName = false)
    }

  def aritySafeParamConversion: String =
    if (arity <= 22)
      params.map { p =>

        val cast =
          if (p._3.isPrimitive && !p._4)
            CodeGenerator.boxedType(p._3.getSimpleName)
          else
            p._1
        val (arrayExtraDim) =
          if (p._3.isArray)
            ("[]")//
          else
            ("")

        s"${p._2} = ($cast$arrayExtraDim) ${p._2}_ppp;"
      }.mkString("\n")
    else
      params.zipWithIndex.map {
        case (p, index) =>
          val cast =
            if (p._3.isPrimitive && !p._4)
              CodeGenerator.boxedType(p._3.getSimpleName)
            else
              p._1
          val (arrayExtraDim) =
            if (p._3.isArray)
              ("[]")// [0]
            else
              ("")

          s"${p._2} = ($cast$arrayExtraDim) ((Object[])input_ppp)[$index];"
      }.mkString("\n")

  protected[quality] var paramCallObject = ""

  def aritySafeParamCallPrep(ctx: CodegenContext): String = {
    paramCallObject = ctx.addMutableState("Object[]", "paramCallAr", v => s"$v = new Object[${params.size}];")
    params.zipWithIndex.map {
      case (p, index) =>
        s"$paramCallObject[$index] = ${p._2};"
    }.mkString("\n")
  }

  def aritySafeParamCall: String =
    if (arity <= 22)
      outerCallParams
    else
      paramCallObject
}

object Params {

  def stripBrackets(v: VariableValue): (String, String) = {
    val openb = v.toString().indexOf("[")
    if (openb == -1)
      (v.variableName, "")
    else
      (v.variableName.dropRight(v.length - openb), v.variableName.drop(openb))
  }

  def formatParams(ctx: CodegenContext, oa: Seq[ExprValue], additional: Seq[ExprValue] = Seq.empty, callsKeepArrays: Boolean = false): ParameterInformation = {

    // split compilation requires the outerscope, this can be very buried and it is not always working with BooleanGrouperTest^s
    // nested case statement failing when sourced from a file

    val a = (oa ++ ctx.currentVars.map(_.value) ++ ctx.currentVars.map(_.isNull)).distinct

    def filterOutArrays(use: Seq[ExprValue]) = use.flatMap {
      case a: VariableValue => Some(a)
      case _ => None
    }

    val filteredA = filterOutArrays(a)
    val filteredAdditional = filterOutArrays(additional)

    val size = filteredA.size + filteredAdditional.size
    val use =
      if (size <= 22)
        filteredA ++ filteredAdditional
      else
        filteredA // additional are then handled via class level

    // filter out any top level arrays, the input is a set, so params need the same order
    val ordered = use

    def prepFields(ordered: Seq[VariableValue]) =
      ordered.map { v =>
        val (stripped, arrayInName) = stripBrackets(v)

        val (typ, array) =
          if (v.javaType.isArray)
            (s"${v.javaType.getComponentType.getName}", "[]")
          else if (v.javaType.isPrimitive)
            (v.javaType.toString, arrayInName.replaceAll("[^\\[\\]]",""))
          else
            (v.javaType.getName, arrayInName.replaceAll("[^\\[\\]]",""))

        (s"$typ$array", stripped, v.javaType, array.nonEmpty) // if it's not empty we want to pass through
      }.distinct

    val pairs = prepFields(ordered)

    val combined =
      if (size <= 22)
        pairs
      else
        pairs ++ prepFields(filteredAdditional)

    val paramsCall =
      ordered.map(v =>
        if (v.javaType.isArray && callsKeepArrays)
          v.variableName
        else
          stripBrackets(v)._1
      ).distinct.mkString(", ")

    ParameterInformation(pairs.map {
      case (typ, stripped, _, _) =>

        s"$typ $stripped"
      }.distinct.mkString(", ")
      , paramsCall, combined.size, combined, outerCallParams = paramsCall, nonCombinedParams = pairs)
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

