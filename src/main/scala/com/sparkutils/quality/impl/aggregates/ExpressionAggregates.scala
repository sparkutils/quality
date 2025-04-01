package com.sparkutils.quality.impl.aggregates

import com.sparkutils.quality.QualityException.qualityException
import eu.timepit.refined.boolean.False
import org.apache.spark.sql.QualitySparkUtils
import org.apache.spark.sql.ShimUtils.cast
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast, Expression, If, LambdaFunction, Literal, NamedLambdaVariable}
import org.apache.spark.sql.qualityFunctions._
import org.apache.spark.sql.types.{DataType, DecimalType, MapType}
import org.apache.spark.sql.ShimUtils.{cast => castf}

object AggregateExpressions {

  def transformSumType(function: Expression, param: NamedLambdaVariable, newDT: DataType) =
    function.transform {
      case n: NamedLambdaVariable if n.exprId == param.exprId =>
        n.copy(dataType = newDT)
      // spark for decimals already gets the type wrong for 0.7.1 syntax type and double wraps it (only checking single breaks deprecated syntax), so
      // ugly workarounds for types not matching.. see above comment
      case cast: Cast if cast.child.isInstanceOf[Cast] && cast.child.asInstanceOf[Cast].child.isInstanceOf[NamedLambdaVariable] =>
        val nvl = cast.child.asInstanceOf[Cast].child.asInstanceOf[NamedLambdaVariable]
        if (nvl.exprId == param.exprId)
          castf(castf(nvl.copy(dataType = newDT), newDT), cast.dataType)
        else
          cast
      // for dbr > 11.2, the cast is on the variable not the expression
      case cast: Cast if cast.child.isInstanceOf[NamedLambdaVariable] =>
        val nvl = cast.child.asInstanceOf[NamedLambdaVariable]
        if (nvl.exprId == param.exprId)
          castf( child = cast.child.asInstanceOf[NamedLambdaVariable].
            copy(dataType = newDT), newDT)
        else
          cast

    }

  /**
   * Should only be created from within the RuleRunnerObject functions, not doing so will lead to unresolved functions and lambda variables - use sql strings only to construct.
   *
   * @param sumType when non-null it triggers a rewrite of both sum and evaluate expressions to set their parameter types to sumType, otherwise it takes those specified in the expressions already
   * @param ifExpr count if true/1 etc.
   * @param sum called lambda expression when ifExpr is true, with the current sum value and returns the new sum value (e.g. current -> current + col2 would total all col2's)
   * @param evaluate combines the count and overall result of combinations in lambda to provide the overall results (e.g. (current, count) -> current / count)
   * @param zero     lookup function to get the zero value for the sum type
   * @param notYetResolved additional casts are needed for the dsl due to decimal precision handling.
   *                 adding them in at the dsl level, however, prevents resolving the lambdas, using Column cast doesn't work either, so they are added in the expression itself
   *                 As the lambda's won't be resolved when calling this the existing matching for the expr sql variant cannot work.
   */
  def apply(sumType: DataType, ifExpr: Expression, sum: Expression, evaluate: Expression, zero: DataType => Option[Any], add: DataType => Option[( Expression, Expression ) => Expression], notYetResolved: Boolean = false ): Expression = {
    /*
     * in the case of decimal's being used the DecimalPrecision analysis can change the types such that the
     * precision is ignored e.g.
     * sumWith('DECIMAL(38,18)', entry -> entry + field)
     * may end up with 38,17 for the lambda despite 38,17 being the input as such this must be casted.
     */

    lazy val correctedSum: Expression =
      if (notYetResolved)
        sum
      else
        correctSum(sumType, sum)

    lazy val correctedEvaluate: Expression =
      if (notYetResolved)
        evaluate
      else
        correctEvaluate(sumType, evaluate)

    val (SeqArgs(sum1 +: _, _), SeqArgs(Seq(sum2, count), _), useSum, useEvaluate) =
      if (sumType eq null)
        (sum, evaluate, sum, evaluate)
      else {
        val temp = (correctedSum, correctedEvaluate, correctedSum, correctedEvaluate)
        temp
      }

    ExpressionAggregates(Seq(count, sum1, sum2, ifExpr, useSum, useEvaluate), zero, add, sumType).toAggregateExpression()
  }

  def correctEvaluate(sumType: DataType, evaluate: Expression, wrapCastOnly: Boolean = false) =
    evaluate match {
      case FunN(Seq(RefExpression(_, nullable, _), cref),
      LambdaFunction(function, Seq(sparam: NamedLambdaVariable, cparam: NamedLambdaVariable), hidden),
      name, _, _, _) =>

        val correctedFunction =
          if (wrapCastOnly)
            function
          else
            transformSumType(function, sparam, sumType)

        FunN(Seq(RefExpression(sumType, nullable), cref),
          LambdaFunction(correctedFunction,
            Seq(sparam.copy(dataType = sumType), cparam), hidden), name)

      // when we partially apply the lambda is further down
      case FunForward(Seq(RefExpression(_, nullable, paramIndex), cref) :+ (
        i@FunN(funparams, LambdaFunction(function, params, hidden), _, _, _, _)
        )) =>
        // params can be longer and not 1:1 due to placeholders
        val sparam = params(paramIndex).asInstanceOf[NamedLambdaVariable]
        val correctedFunction =
          if (wrapCastOnly)
            function
          else
            transformSumType(function, sparam, sumType)

        FunForward(Seq(RefExpression(sumType, nullable, paramIndex), cref) :+
          i.copy(function = LambdaFunction(correctedFunction,
            sparam.copy(dataType = sumType) +: params.drop(1), hidden),
            arguments = funparams.updated(paramIndex,
              funparams(paramIndex).asInstanceOf[RefExpression].copy(dataType = sumType)
            )))

      case _ => evaluate // shouldn't happen but better from spark than a match error
    }

  def correctSum(sumType: DataType, sum: Expression, wrapCastOnly: Boolean = false) =
    sum match {
      // re-pack with new type

      // for sumWith
      case FunN(Seq(RefExpression(_, nullable, _)),
      LambdaFunction(fun, Seq(param: NamedLambdaVariable), hidden),
      name, _, _, _) =>

        val correctedFunction =
          if (wrapCastOnly)
            fun
          else
            transformSumType(fun, param, sumType)

        FunN(Seq(RefExpression(sumType, nullable)),
          LambdaFunction(cast(correctedFunction, sumType), // see above comment for cast justification
            Seq(param.copy(dataType = sumType)), hidden), name)

      // for mapWith
      case MapTransform(r: RefExpression, key, LambdaFunction(function, Seq(param: NamedLambdaVariable), hidden), zeroF) =>

        val MapType(_, valueType, _) = sumType

        val correctedFunction =
          if (wrapCastOnly)
            function
          else
            transformSumType(function, param, valueType)

        MapTransform(RefExpression(sumType, r.nullable), key,
          LambdaFunction(cast(correctedFunction, valueType), // see above comment for cast justification
            Seq(param.copy(dataType = valueType)), hidden), zeroF)

      // when we partially apply the lambda is further down
      case FunForward(Seq(RefExpression(_, nullable, paramIndex)) :+ (
        i@FunN(funparams, LambdaFunction(function, params, hidden), _, _, _, _)
        )) =>
        // params can be longer and not 1:1 due to placeholders
        val sparam = params(paramIndex).asInstanceOf[NamedLambdaVariable]
        val correctedFunction =
          if (wrapCastOnly)
            function
          else
            transformSumType(function, sparam, sumType)

        FunForward(Seq(RefExpression(sumType, nullable, paramIndex)) :+
          i.copy(function = LambdaFunction(cast(correctedFunction, sumType), // see above comment for cast justification
            sparam.copy(dataType = sumType) +: params.drop(1), hidden),
            arguments = funparams.updated(paramIndex,
              funparams(paramIndex).asInstanceOf[RefExpression].copy(dataType = sumType)
            )))

      case _ => sum // not expected but better errors will come form Spark
    }
}

/**
 * Represents an aggregation expression built from a filter function, a sum lambda and an evaluate lambda which uses the
 * count of filter hits and the sum as parameters.
 * @param children
 */
case class ExpressionAggregates(override val children: Seq[Expression], zero: DataType => Option[Any], addF: DataType => Option[( Expression, Expression ) => Expression], sumType: DataType) extends DeclarativeAggregate {
  // extending higherorder fun is needed otherwise case other => other.failAnalysis( is thrown in analysis/higherOrderFunctions
  lazy val Seq(countLeaf: RefExpression, sumLeaf: RefExpression, evalSumLeaf: RefExpression, ifExpr, sumWith, evaluate) =
    if (children.forall(_.resolved))
      rewriteChildren(children)
    else
      children

  private def rewriteChildren(children: Seq[Expression]) = {
    import AggregateExpressions.{correctEvaluate, correctSum}
    val Seq(_, _, _, ifExpr, sumWith, evaluate) = children

    val correctedEvaluate = correctEvaluate(sumType, evaluate, wrapCastOnly = true)
    val correctedSum = correctSum(sumType, sumWith, wrapCastOnly = true)

    val (SeqArgs(sum1 +: _, _), SeqArgs(Seq(sum2, count), _), useSum, useEvaluate) =
      if (sumType eq null)
        (sumWith, evaluate, sumWith, evaluate)
      else {
        val temp = (correctedSum, correctedEvaluate, correctedSum, correctedEvaluate)
        temp
      }

    Seq(count, sum1, sum2, ifExpr, useSum, useEvaluate)
  }

  lazy val sumRef = AttributeReference("sum", sumLeaf.dataType, true)()
  lazy val countRef = AttributeReference("count", countLeaf.dataType)()

  lazy val sum = RefSetterExpression(Seq(sumLeaf, sumRef))
  lazy val sumEval = RefSetterExpression(Seq(evalSumLeaf, sumRef))
  lazy val count = RefSetterExpression(Seq(countLeaf, countRef))

  lazy val runEvaluate = RunAllReturnLast(Seq(sumEval, count, evaluate))
  lazy val sumWithEvaluate = RunAllReturnLast(Seq(sum, sumWith))

  lazy val add = addF(sumWith.dataType).getOrElse(qualityException(s"Cannot find the monoidal add for type ${sumWith.dataType}"))

  override lazy val initialValues: Seq[Expression] = Seq(
    Literal(0L), // count
    {
      val dt = sumLeaf.dataType
      val zeroE = zero(dt).getOrElse(qualityException(s"Could not find zero for type ${dt}"))
      Literal(zeroE, dt)
    }
  )

  override lazy val updateExpressions: Seq[Expression] = Seq(
    If(ifExpr, count + 1L, count),
    If(ifExpr, sumWithEvaluate, sum)
  )

  override lazy val mergeExpressions: Seq[Expression] = Seq(
    count.left + count.right,
    sumLeaf.dataType match {
      case _: DecimalType => cast(add(sum.left, sum.right), sumLeaf.dataType) // extra protection against SPARK-39316
      case _ => add(sum.left, sum.right)
    }
  )

  override lazy val evaluateExpression: Expression = runEvaluate

  override lazy val aggBufferAttributes: Seq[AttributeReference] = Seq(countRef, sumRef)

  override def dataType: DataType = evaluate.dataType

  override def nullable: Boolean = false

  implicit class RichAttribute(a: RefSetterExpression) {
    /** Represents this attribute at the mutable buffer side. */
    def left: RefSetterExpression = a

    /** Represents this attribute at the input buffer side (the data value is read-only). */
    def right: AttributeReference = inputAggBufferAttributes(aggBufferAttributes.indexOf(a.from))
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)
}
