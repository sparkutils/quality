package com.sparkutils.quality.impl.util

import com.sparkutils.quality._
import com.sparkutils.quality.impl.util.ParameterInformation.isCodeGenParameter
import com.sparkutils.quality.impl.util.Params.{prepFields, stripBrackets}
import com.sparkutils.quality.impl.{RuleLogicUtils, ThreeOnlyNonFoldable}
import net.jpountz.lz4.{LZ4BlockInputStream, LZ4BlockOutputStream, LZ4Factory}
import net.jpountz.xxhash.XXHashFactory
import org.apache.spark.SparkConf
import org.apache.spark.internal.config.IO_COMPRESSION_LZ4_BLOCKSIZE
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.QualityCodeGenUtils.{isProbablyLocalCompilationScope, isProbablyLocalScope}
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
  def withT[R](t: T)(thunk: => R): R = {
    if (threadLocal eq null) {
      get() // init
    }
    threadLocal.set(t)
    try {
      thunk
    } finally {
      threadLocal.remove()
    }
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

case class ParamType(typeDecl: String, name: String, classType: Class[_], isExtraDimensionArray: Boolean,
                     isBoxed: Boolean, isLocal: Boolean)

object ParameterInformation {

  val forMerging: ParameterInformation = ParameterInformation("","",0,Seq.empty)

  def isCodeGenParameter(ctx: CodegenContext)(p: ParamType): Boolean =
    isCodeGenParameterS(ctx)(p.name)

  private def exprValueMatches(expr: ExprValue, Name: String) =
    if (expr eq null)
      false
    else
      expr match {
        case VariableValue(Name,_) => true
        case _ => false
      }

  def isCodeGenParameterS(ctx: CodegenContext)(Name: String): Boolean =
    (ctx.currentVars ne null) && ctx.currentVars.exists(ex =>
      if (ex eq null)
        false
      else
        exprValueMatches(ex.value, Name) || exprValueMatches(ex.isNull, Name)
    )

}

/**
 * Additional is pass through and is used to bubble up parameters to higher level callers
 * @param paramsDef drop in for function lists
 * @param paramsCall drop in for function calls
 * @param arity the arity of the parameters, abstract function only goes to 22, 255 are available
 * @param pushToTop any outer context information (spark 3.1 and higher)
 * @param params pairs of variable name to java type used for declaration and the class type for boxing
 * @param topLevelRunnerParams top level params typically created by the runner to push down, each sub compilation unit
 *                             will have them as mutable state through aritySafe
 */
case class ParameterInformation(paramsDef: String, paramsCall: String, arity: Int,
                                params: Seq[ParamType], pushToTop: String = "",
                                outerCallParams: String = "",
                                // split expressions pairs
                                nonCombinedParams: Seq[ParamType] = Seq.empty,
                                returnTyp: String = "Object",//"InternalRow"
                                topLevelRunnerParams: Seq[(VariableValue, Boolean)] = Seq.empty,
                                preppedTopLevel: Seq[ParamType] = Seq.empty
                               ) {

  /**
   * Creates an "uber" seq of params and nonCombinedParams for inputadapters (e.g. row attributes) and bumps arity
   * along with refreshing outerCallParams,
   * all other variables are kept and should be
   * treated as unusable.  other.additionalParams is needed to thread extra runner added state through when calling
   * genCompilerTerms
   * @param ctx should be the current ctx and is used only to evaluate localscope
   * @param other
   * @param topLevel if it's toplevel we do not use other.additionalParams as the next compilation unit creates them
   * @return
   */
  def mergeParams(ctx: CodegenContext, other: ParameterInformation, topLevel: Boolean): ParameterInformation = {
    //println("mergeParams other names: " + other.params.map(_.name))
    val prepped =
      if (preppedTopLevel.nonEmpty) // prepped need to remove additional arrays
        preppedTopLevel
      else
        if (topLevel)
          Seq.empty
        else
          other.preppedTopLevel

    // input adapters are needed to pipe the Spark row generation through
    // local variables from subExpr code in the 'apply/processNext' may be needed for further calls
    val nparams = (params ++
      other.params.filter(isCodeGenParameter(ctx))
      ).distinct

    val nonLocalParams = nparams.filterNot(_.isLocal)

    val top =
      if (topLevelRunnerParams.nonEmpty)
        topLevelRunnerParams // grouped folder / collector
      else
        if (topLevel)
          Seq.empty
        else
          other.topLevelRunnerParams // non grouped

    copy(params = nparams,
      nonCombinedParams = (nonCombinedParams ++ other.nonCombinedParams.filter(isCodeGenParameter(ctx))).distinct,
        arity = (
          if (preppedTopLevel.nonEmpty) // prepped need to remove additional arrays
            (nonLocalParams ++ preppedTopLevel).distinct.size
          else
            if (topLevel)
              nonLocalParams.size
            else
              (nonLocalParams ++ other.preppedTopLevel).distinct.size
          ),
      outerCallParams = (nonLocalParams.map(_.name) ++ prepped.map(_.name)).distinct.mkString(", "),
      topLevelRunnerParams = top,
      preppedTopLevel = prepFields(ctx, top, true)
    )
  }

  def useArity = if (arity > 22) 1 else aritySafe.length

  /**
   * When arity is over 22 we still need a type, so the type becomes an array we unpack..., boxing is unavoidable
   *
   * Arity of 0 implies no actual input information is needed, e.g. a rule is hardcode / folded to a constant
   *
   * @return
   */
  def aritySafeApplyType(prefix: String): String =
    s"$prefix$useArity<$returnTyp${if (arity > 0) "," else ""}" +
      (
        if (arity <= 22)
          aritySafe.map { p => "Object"
/*            if (p._3.isPrimitive)
              CodeGenerator.boxedType(p._3.getSimpleName)
            else
              p._1*/
          }.mkString(",")
        else
          "Object"
        ) + ">"

  var aritySafe: Seq[ParamType] = _

  def aritySafeParamDef: String =
    if (arity <= 22)
      aritySafe.map{ p=>
        s"Object ${p.name}_ppp" // only object will compile, janino no generics
      }.distinct.mkString(",")
    else
      "Object input_ppp"

  def addAritySafeParamDecl(ctx: CodegenContext): Unit = {
    // any locally created (in apply) subexprs should not be in the arity
    aritySafe = (params ++ preppedTopLevel).distinct

    aritySafe.map { p =>
      val (arrayExtraDecl, arrayExtraDim) =
        if (p.isExtraDimensionArray)
          (p.name, "[]") // s" = new ${p._3.componentType().getName}[1][]
        else
          (p.name, "")

      val typ =
        if (p.isBoxed && p.classType.isArray)
          CodeGenerator.boxedType(p.classType.getComponentType.getSimpleName) + "[]"
        else
          CodeGenerator.typeName(p.classType)

      ctx.addMutableState(typ+arrayExtraDim, p.name, forceInline = true, useFreshName = false)
    }
  }

  def aritySafeParamConversion(ctx: CodegenContext): String =
    if (arity <= 22)
      aritySafe.map { p =>

        val cast =
          if (p.isBoxed && p.classType.isArray)
            CodeGenerator.boxedType(p.classType.getComponentType.getSimpleName) + "[]"
          else
            if (p.classType.isPrimitive && !p.isExtraDimensionArray)
              CodeGenerator.boxedType(p.classType.getSimpleName)
            else
              p.typeDecl

        val (arrayExtraDim) =
          if (p.classType.isArray)
            ("[]")//
          else
            ("")

        s"${p.name} = ($cast$arrayExtraDim) ${p.name}_ppp;"
      }.mkString("\n")
    else {
      val pp = ctx.freshName("ppp_ar")
      s"""
        Object[] $pp = (Object[])input_ppp;
         """ +
      params.filterNot(_.isLocal).zipWithIndex.map {
        case (p, index) =>
          val cast =
            if (p.classType.isPrimitive && !p.isExtraDimensionArray)
              CodeGenerator.boxedType(p.classType.getSimpleName)
            else
              p.typeDecl
          val (arrayExtraDim) =
            if (p.classType.isArray)
              ("[]")// [0]
            else
              ("")

          s"${p.name} = ($cast$arrayExtraDim) $pp[$index];"
      }.mkString("\n")
    }

  protected[quality] var paramCallObject = ""

  def aritySafeParamCallPrep(outerCtx: CodegenContext, ctx: CodegenContext): String =
    if (arity <= 22) "" else {
      // locals do not exist and are only for this compilation unit not parents
      val outerSafeParams = aritySafe//.filterNot(p => QualityCodeGenUtils.isProbablyLocalCompilationScope(ctx, p.name))
      paramCallObject = outerCtx.addMutableState("Object[]", "paramCallAr", v => s"$v = new Object[${outerSafeParams.size}];")
      outerSafeParams.zipWithIndex.map {
        case (p, index) =>
          s"$paramCallObject[$index] = ${p.name};"
      }.mkString("\n")
    }

  def aritySafeParamCall: String =
    if (arity <= 22)
      outerCallParams
    else
      paramCallObject

  def topLevelCall(str: String, outerParams: ParameterInformation) =
    if (outerParams.topLevelRunnerParams.nonEmpty)
      str + "," + outerParams.preppedTopLevel.map(_.name).mkString(",")
    else
      str

  def outerParamsCall(outerParams: ParameterInformation): String =
    topLevelCall(paramsCall, outerParams)
}

object Params {

  def stripBrackets(v: VariableValue): (String, String) = {
    val openb = v.toString().indexOf("[")
    if (openb == -1)
      (v.variableName, "")
    else
      (v.variableName.dropRight(v.length - openb), v.variableName.drop(openb))
  }

  def prepFields(ctx: CodegenContext, ordered: Seq[(VariableValue, Boolean)], runnerParams: Boolean = false,
                 additionalParams: Seq[(VariableValue, Boolean)] = Seq.empty): Seq[ParamType] = {
    val isAdditionalParams = additionalParams.map(p => stripBrackets(p._1)._1).toSet

    ordered.map { case (v, box) =>
      val (stripped, arrayInName) = stripBrackets(v)

      val (typ, array) =
        if (v.javaType.isArray)
          (s"${v.javaType.getComponentType.getName}", "[]")
        else if (v.javaType.isPrimitive)
          (v.javaType.toString, arrayInName.replaceAll("[^\\[\\]]",""))
        else
          (v.javaType.getName, arrayInName.replaceAll("[^\\[\\]]",""))

      ParamType(s"$typ$array", stripped, v.javaType, array.nonEmpty, box,
        if (runnerParams || isAdditionalParams(stripped))
          false // these *are* local but are handled outside of the stack
        else
          isProbablyLocalScope(ctx, stripped)
      ) // if it's not empty we want to pass through
    }.distinct
  }

  def formatParams(ctx: CodegenContext, a: Seq[ExprValue], additional: Seq[(VariableValue, Boolean)] = Seq.empty, callsKeepArrays: Boolean = false): ParameterInformation = {

    def filterOutArrays(use: Seq[ExprValue]) = use.flatMap {
      case a: VariableValue => Some((a, false))
      case _ => None
    }
    def filterOutArraysB(use: Seq[(ExprValue,Boolean)]) = use.flatMap {
      case (a: VariableValue, b) => Some((a, b))
      case _ => None
    }

    val filteredA = filterOutArrays(a)
    val filteredAdditional = filterOutArraysB(additional)

    val size = filteredA.size + filteredAdditional.size
    val use =
      //if (size <= 22)
        filteredA ++ filteredAdditional
      //else
        //filteredA // additional are then handled via class level

    // filter out any top level arrays, the input is a set, so params need the same order
    val ordered = use

    val pairs = prepFields(ctx, ordered, additionalParams = additional)

    val combined =
      //if (size <= 22)
        pairs
      //else
        //pairs ++ prepFields(filteredAdditional)

    def paramsCall(seq: Seq[(VariableValue, Boolean)]) =
      seq.map { case (v, primitive) =>
        if (v.javaType.isArray && callsKeepArrays)
          v.variableName
        else
          stripBrackets(v)._1
      }.distinct.mkString(", ")

    ParameterInformation(pairs.map {
      case ParamType(typ, stripped, _, _, _, _) =>

        s"$typ $stripped"
      }.distinct.mkString(", ")
      , paramsCall(ordered), combined.size, combined,
      outerCallParams = paramsCall(ordered.filterNot(p => isProbablyLocalCompilationScope(ctx, p._1.variableName))),
      nonCombinedParams = pairs)
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

class Counter() {
  var counter = 0

  def next(): Int = {
    counter += 1
    counter
  }
}