package org.apache.spark.sql.qualityFunctions

import com.sparkutils.quality.QualityException
import com.sparkutils.quality.impl.RuleLogicUtils
import com.sparkutils.shim.expressions.HigherOrderFunctionLike
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, HigherOrderFunction, LambdaFunction, LeafExpression, NamedExpression, NamedLambdaVariable, OuterReference, SubqueryExpression, UnresolvedNamedLambdaVariable}
import org.apache.spark.sql.qualityFunctions.SubQueryLambda.namedToOuterReference
import org.apache.spark.sql.types.{AbstractDataType, DataType}

import java.util.concurrent.atomic.AtomicReference

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

trait RefCodeGen {
  def dataType: DataType
  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
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

/**
 * Getter, trimmed version of NamedLambdaVariable as it should never be resolved
 * @param dataTypeF
 * @param nullable
 */
case class RefExpressionLazyType(dataTypeF: AtomicReference[DataType],
                                 nullable: Boolean, _resolved: Boolean = true)
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

  lazy val params :+ (function @ FunN(args, fun, _, _, _)) = children

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

/**
 * Lambda function with multiple args, typically created with a placeholder AtomicRefExpression args
 *
 * @param arguments Evaluated to provide input to the function lambda
 * @param function the actual lambda function
 * @param name the lambda name when available
 */
case class FunN(arguments: Seq[Expression], function: Expression, name: Option[String] = None,
                processed: Boolean = false, attemptCodeGen: Boolean = false)
  extends HigherOrderFunctionLike with CodegenFallback with SeqArgs with FunDoGenCode {

  override def prettyName: String = name.getOrElse(super.prettyName)

  override def argumentTypes: Seq[AbstractDataType] = arguments.map(_.dataType)

  override def functions: Seq[Expression] = Seq(function)

  override def functionTypes: Seq[AbstractDataType] = Seq(function.dataType)

  protected def bindInternal(f: (Expression, Seq[(DataType, Boolean)]) => LambdaFunction): HigherOrderFunction = {
    // subqueries aren't being replaced correctly
    val res = copy(function = f(function,
        arguments.map(e => (e.dataType, e.nullable))))

    if (RuleLogicUtils.hasSubQuery(res.function)) {
      // only possible on > 3.4 (and DBR 12.2),
      // no longer possible after 14.3/4.0, this code won't be reached due to https://issues.apache.org/jira/browse/SPARK-47509
      // unless it's re-enabled
      // given XX below reject this occurrence directly.
      if (!arguments.forall(_.collect{case u: UnresolvedNamedLambdaVariable => u}.isEmpty)) {
        QualityException.qualityException(s"Cannot use LambdaFunctions with SubqueryExpressions and parameters containing lambdavariables " + this)
      }

      val converted = SubQueryLambda.convertLambdaFunction(res.function)(function, arguments)

      res.copy(function = converted)
    } else
      res

  }

  @transient lazy val LambdaFunction(lambdaFunction, elementNamedVariables, _) = function
  @transient lazy val elementVars = elementNamedVariables.map(_.asInstanceOf[NamedLambdaVariable])

  override def eval(inputRow: InternalRow): Any = {
    // set up the variable to be evaluated
    elementVars.zip(arguments).foreach{ case (element, expr) =>
      element.value.set(expr.eval(inputRow))
    }

    function.eval(inputRow)
  }

  override def checkInputDataTypes(): TypeCheckResult = TypeCheckResult.TypeCheckSuccess

  override def dataType: DataType = function.dataType

  override def children: Seq[Expression] = arguments ++ functions
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