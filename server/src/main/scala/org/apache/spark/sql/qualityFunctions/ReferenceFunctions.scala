package org.apache.spark.sql.qualityFunctions

import com.sparkutils.quality.QualityException
import com.sparkutils.quality.impl.util.TSLocal
import com.sparkutils.quality.impl.{ExpressionCompiler, RuleLogicUtils}
import com.sparkutils.shim.expressions.HigherOrderFunctionLike
import com.sparkutils.testing.SparkVersions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.{Expression, HigherOrderFunction, LambdaFunction, LeafExpression, NamedExpression, NamedLambdaVariable, OuterReference, SubqueryExpression, UnresolvedNamedLambdaVariable}
import org.apache.spark.sql.types.{AbstractDataType, BooleanType, DataType}

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable

/**
 * Wraps other expressions and stores the result in an RefExpression -
 */
case class RefSetterExpression(children: Seq[Expression]) extends Expression
  with CodegenFallback {

  lazy val Seq(atomic: RefExpression, from: Expression) = children

  override def eval(input: InternalRow): Any = {
    val res = from.eval(input)
    atomic.value = res
    res
  }

  override def nullable: Boolean = from.nullable

  override lazy val resolved: Boolean = true

  override def dataType: DataType = from.dataType

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(children = newChildren)
}

/**
 * Runs all of the children and returns the last's eval result - allows stitching together lambdas with aggregates
 * @param children
 */
case class RunAllReturnLast(children: Seq[Expression]) extends Expression
  with CodegenFallback {

  lazy val runAll = children.dropRight(1)
  lazy val ret = children.last

  override def eval(input: InternalRow): Any = {
    runAll.foreach(_.eval(input))

    ret.eval(input)
  }

  override def nullable: Boolean = false

  override lazy val resolved: Boolean = true

  override def dataType: DataType = ret.dataType

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(children = newChildren)

}

trait CacheApproach {
  def getOrBuild(ctx: CodegenContext)(genCode: CodegenContext => ExprCode): ExprCode
}

case class MapBasedCacheApproach() extends CacheApproach {
  @transient
  var map = mutable.Map[CodegenContext, ExprCode]()

  override def getOrBuild(ctx: CodegenContext)(genCode: CodegenContext => ExprCode): ExprCode = {
    if (map == null) {
      map = mutable.Map[CodegenContext, ExprCode]()
    }
    val cached = map.get(ctx)
    if (cached.isEmpty) {
      val toCache = genCode(ctx)
      map.put(ctx, toCache)
      toCache
    } else {
      cached.get
    }
  }
}

case class OptionCacheApproach() extends CacheApproach {
  @transient
  var opt: Option[ExprCode] = None

  override def getOrBuild(ctx: CodegenContext)(genCode: CodegenContext => ExprCode): ExprCode = {
    if (opt == null) {
      opt = None
    }
    if (opt.isEmpty) {
      val toCache = genCode(ctx)
      opt = Some(toCache)
      toCache
    } else {
      opt.get
    }
  }
}

object RefCodeGen {
  private val cacheApproach = TSLocal[() => CacheApproach]( () => () => MapBasedCacheApproach() )
  def withCacheApproach[R](t: () => CacheApproach)(thunk: => R): R = {
    cacheApproach.withT( t )(thunk)
  }
}

trait RefCodeGen extends LambdaVariablePattern {
  def dataType: DataType

  // never return a different object from this gen code
  @transient
  var _generated: CacheApproach = null

  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    if (ExpressionCompiler.inExpressionCompiler) {
      // evals are compiled but ruleFolder is eval, so forceInterpreted style scenarios
      val idx = ctx.references.length
      ctx.references += this
      val javaType = CodeGenerator.javaType(dataType)
      val boxed = CodeGenerator.boxedType(dataType)

      val clazz = this.getClass.getName
      ev.copy(code =
        code"""
        $javaType ${ev.value} = ($boxed) (($clazz)references[$idx]).value();

        boolean ${ev.isNull} = ${ev.value} == null;
      """)
    } else {
      if (_generated == null){
        _generated = RefCodeGen.cacheApproach.get().apply()
      }

      _generated.getOrBuild(ctx) { ctx =>
        val javaType = CodeGenerator.javaType(dataType)
        val theVar = ctx.addMutableState(javaType, ctx.freshName("RefExpr"), useFreshName = false)
        val theNull = ctx.addMutableState("boolean", ctx.freshName("RefExprNull"), useFreshName = false)

        val toCache = ev.copy(code = code"",
          isNull = VariableValue(theNull, CodeGenerator.javaClass(BooleanType)),
          value = VariableValue(theVar, CodeGenerator.javaClass(dataType))
        )
        toCache
      }
    }
}

/**
 * Getter, trimmed version of NamedLambdaVariable as it should never be resolved
 * @param dataType
 * @param nullable
 */
case class RefExpression(dataType: DataType,
                         nullable: Boolean = true, index: Int = -1)
  extends LeafExpression with RefCodeGen {

  var value: Any = _

  override def eval(input: InternalRow): Any = value

  override lazy val resolved: Boolean = true

}

object RefExpressionLazyType {
  /**
   * withNewChildren on 3.2 and above works correctly for resolution allowing a transform before creating.
   */
  lazy val defaultResolved: Boolean = {
    val bits = SparkVersions.sparkVersion.split('.')
    val ver = s"${bits(0)}${bits(1)}".toInt
    ver <= 32
  }
}

/**
 * Allows threading types from child resolves, the type is atomic but the value is not
 * @param dataTypeF
 * @param nullable
 */
case class RefExpressionLazyType(
  dataTypeF: AtomicReference[DataType],
  nullable: Boolean,
  _resolved: Boolean = RefExpressionLazyType.defaultResolved)
  extends LeafExpression with RefCodeGen {

  var value: Any = _

  override def eval(input: InternalRow): Any = value

  override lazy val resolved: Boolean = _resolved

  def dataType: DataType = dataTypeF.get()

}

object SeqArgs {
  def unapply(expression: Expression): Option[(Seq[Expression], Expression)] =
    expression match {
      case s : SeqArgs => Some((s.arguments, s.function))
      case f : FunForward => Some((f.children.dropRight(1), f.children.last))
      case _ => None
    }
}

trait SeqArgs {
  def arguments: Seq[Expression]
  def function: Expression
}

/**
 * Forwards calls to the function arguments via setters.  This is
 * only evaluated in aggExpr, all other usages are removed during lambda
 * creation.
 *
 * This removal may also be forced in aggExpr at a later stage
 */
case class FunForward(children: Seq[Expression])
  extends Expression with CodegenFallback {

  lazy val params :+ (function @ FunN(args, fun, _, _, _, _)) = children

  def nullable: Boolean = function.nullable

  def eval(input: InternalRow): Any = {
    // each of the params are RefExpression's
    // set up the variable to be evaluated
    params.foreach{ case param =>
      val p = param.asInstanceOf[RefExpression]
      args(p.index).
        asInstanceOf[RefExpression].value = p.value
    }

    function.eval(input)
  }

  def dataType: DataType = function.dataType

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(children = newChildren)
}

trait Binder extends HigherOrderFunctionLike {

  def arguments: Seq[Expression]

  def function: Expression

  override def children: Seq[Expression] = arguments ++ functions

  def argumentTypes: Seq[AbstractDataType] = arguments.map(_.dataType)

  def functions: Seq[Expression] = Seq(function)

  def functionTypes: Seq[AbstractDataType] = Seq(function.dataType)

  def withFunction(function: Expression): HigherOrderFunction with Binder
  def argsToBind: Seq[Expression]

  @transient lazy val LambdaFunction(lambdaFunction, elementNamedVariables, _) = function
  @transient lazy val elementVars = elementNamedVariables

  protected def bindInternal(f: (Expression, Seq[(DataType, Boolean)]) => LambdaFunction): HigherOrderFunction = {
    // subqueries aren't being replaced correctly
    val res = withFunction(function = f(function,
      argsToBind.map(e => (e.dataType, e.nullable))))

    if (RuleLogicUtils.hasSubQuery(res.function)) {
      // only possible on > 3.4 (and DBR 12.2),
      // no longer possible after 14.3/4.0, this code won't be reached due to https://issues.apache.org/jira/browse/SPARK-47509
      // unless it's re-enabled
      // given XX below reject this occurrence directly.
      if (!argsToBind.forall(_.collect{case u: UnresolvedNamedLambdaVariable => u}.isEmpty)) {
        QualityException.qualityException(s"Cannot use LambdaFunctions with SubqueryExpressions and parameters containing lambdavariables " + this)
      }

      val converted = SubQueryLambda.convertLambdaFunction(res.function)(function, argsToBind)

      res.withFunction(function = converted)
    } else
      res

  }

}

/**
 * Swapped out for FunN during SeparateCompilation to ensure pre 4 OSS and up to DBR 18 do not create
 * subexpressions for usedAsLambda FunNs (e.g. collector processing or any folder output expression).
 *
 * Importantly, we _do_ want children to be subexpr eliminated where possible, hence throwaway code.
 * @param funN
 */
case class FunNLambda(funN: FunN) extends Expression {

  override def children: Seq[Expression] = funN.children

  override def nullable: Boolean = funN.nullable

  override def dataType: DataType = funN.dataType

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(funN.withNewChildren(newChildren).asInstanceOf[FunN])

  override def eval(input: InternalRow): Any = ???

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    ev.copy(isNull = TrueLiteral,
      code =
        code"""
          // FunNLambda subExpr
          ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        """)

}

object FunNLambda {
  def swap(expr: Expression): Expression =
    expr match {
      case f: FunN if f.usedAsLambda => FunNLambda(f)
      case _ =>
        expr.transform{
          case f: FunN if f.usedAsLambda => FunNLambda(f)
        }
    }
  def swapBack(expr: Expression): Expression =
    expr match {
      case f: FunNLambda => f.funN
      case _ =>
        expr.transform{
          case f: FunNLambda => f.funN
        }
    }
}

/**
 * Lambda function with multiple args, typically created with a placeholder AtomicRefExpression args
 *
 * @param arguments Evaluated to provide input to the function lambda
 * @param function the actual lambda function
 * @param name the lambda name when available
 */
case class FunN(arguments: Seq[Expression], function: Expression, name: Option[String] = None,
                processed: Boolean = false, attemptCodeGen: Boolean = false, usedAsLambda: Boolean = false)
  extends Binder with CodegenFallback with SeqArgs with FunDoGenCode {

  /* #71 - default just checks arguments, but FunNRewrite will take the actual function so it's possible
      it is nullable. ArrayAggregate for example (hit on Databricks) argument.nullable || finish.nullable
      only the argument part is covered by default HOF nullable.
   */
  override def nullable: Boolean = super.nullable || function.nullable

  override def prettyName: String = name.getOrElse(super.prettyName)

  override def eval(inputRow: InternalRow): Any = {
    // set up the variable to be evaluated
    elementVars.zip(arguments).foreach{ case (element, expr) =>
      val r = expr.eval(inputRow)
      element match {
        case nlv: NamedLambdaVariable => nlv.value.set(r)
        case nlvg: NamedLambdaVariableCodeGen => nlvg.value = r
      }
    }

    function.eval(inputRow)
  }

  override def checkInputDataTypes(): TypeCheckResult = TypeCheckResult.TypeCheckSuccess

  override def dataType: DataType = function.dataType

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren.dropRight(1), newChildren.last)

  /**
   * Called from an initial doGenCode and then by processLambda
   * @param ctx
   * @param ev
   * @return
   */
  def doActualGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    ctx.references += this
    val funExpressionIdx = ctx.references.size - 1
    val funClassName = classOf[FunN].getName
    val args = arguments.map(_.genCode(ctx))
    val fun = lambdaFunction.genCode(ctx)
    val javaType = CodeGenerator.javaType(lambdaFunction.dataType)
    val boxed = CodeGenerator.boxedType(lambdaFunction.dataType)

    val nlvClazz = classOf[NamedLambdaVariable].getName

    val argsetup =
      ((elementNamedVariables.zipWithIndex) zip args).map{p =>
        val ((ev, pos), arg) = p
        if (ev.isInstanceOf[NamedLambdaVariableCodeGen]) {
          // NB The CodegenFallback case can't be tested for as per this very code you are reading
          // it can be swapped out at runtime.  We can test for eval calls but given the call could be behind an
          // if way deep in the stack it's not something easy to optimise out at runtime either.
          val nlv = ev.asInstanceOf[NamedLambdaVariableCodeGen]
          val snippet = s"""
             // gen the arg code
             ${arg.code}
             // capture the result and pass it to the lambdavariable holder ref
             if (!${arg.isNull}) {
                ${nlv.valueRef} = ${arg.value};
             } else {
                ${nlv.valueRef} = null;
             }
             // for cases when the user of the code is CodegenFallback
             ${nlv.genCode(ctx).code}
             """
          snippet
        } else
          s"""
             // nlv compat
             ${arg.code}
             (($nlvClazz)(($funClassName)references[$funExpressionIdx]).elementNamedVariables().apply($pos)).value().set(${arg.value});
             """
      }.mkString("")

    val lambdaName = name.getOrElse("<undefined>")

    // CodegenContext seems to expect
    ev.copy(code =
      code"""
         // Lambda FunN - $lambdaName
         // setup the args
         ${argsetup}
         // gen the function
         ${fun.code}

         // capture the result of the function
         $javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
         boolean ${ev.isNull} = ${fun.isNull};
         if (!${ev.isNull}) {
            ${ev.value} = ($boxed) ${fun.value};
         }
         // End FunN - $lambdaName
          """)
  }

  override def withFunction(function: Expression): HigherOrderFunction with Binder = copy(function = function)

  override def argsToBind: Seq[Expression] = arguments
}

object SubQueryLambda {
  def namedToOuterReference(wrap: Expression) = wrap.transform {
    case n: NamedExpression => OuterReference(n) // replace the NamedLambdaVariable with the reference
    // XX just expression will cause an exception printing the plan and showing the
    // lambda variable is not accessible, wrapping it in OuterReference leads to a useless binding error
  }

  def convertLambdaFunction(potentialLambda: Expression, transformLambdaVariable: Expression => Expression = identity)(oldL: Expression = potentialLambda, inArgs: Seq[Expression] = Seq.empty): Expression =
    oldL match {
      case l: LambdaFunction =>

        val replaced = {
          // get the current args, they are the right ones to potentially replace
          // resolve on the subquery doesn't work for LambdaVariables
          val newL = potentialLambda.asInstanceOf[LambdaFunction]
          val args = if (inArgs.isEmpty) l.arguments else inArgs
          val indexes = l.arguments.zipWithIndex.toMap[Expression, Int]
          val names = newL.arguments.zipWithIndex.map(a => a._1.name -> a._2).toMap

          potentialLambda.transform {
            case s: SubqueryExpression => s.withNewPlan(s.plan.transform {
              case snippet => snippet.transformAllExpressions {
                case a: UnresolvedNamedLambdaVariable =>
                  indexes.get(a).map(i => namedToOuterReference(transformLambdaVariable(args(i)))).getOrElse(a)
                case a: UnresolvedAttribute =>
                  names.get(a.name).map(lamVar => namedToOuterReference(args(lamVar))).getOrElse(a)
              }
            })
          }
        }

        replaced
      case _ =>
        // where there are no params in the lambda
        potentialLambda
    }
}