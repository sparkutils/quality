package org.apache.spark.sql.qualityFunctions

import com.sparkutils.quality.QualityException.qualityException
import com.sparkutils.quality.impl.{LambdaFunction => QLambdaFunction}
import com.sparkutils.shim.expressions.Names.toName
import org.apache.spark.sql.{ShimUtils, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.apache.spark.sql.catalyst.expressions.{Expression, LambdaFunction, LeafExpression, Literal, NamedExpression, NamedLambdaVariable, Unevaluable, UnresolvedNamedLambdaVariable}
import org.apache.spark.sql.qualityFunctions.FunCall.applyFunN
import org.apache.spark.sql.types.{BooleanType, DataType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String


object FunCall {
  /**
   * Force a rewrite of the underlying lambda
   * @param ff
   * @return
   */
  def apply(ff: FunForward): Expression =
    applyFunN(ff).function

  /**
   * Removes placeholders and rewrites LambdaFunctions
   * @param ff
   * @return
   */
  def applyFunN(ff: FunForward, transformFunction: (LambdaFunction, NamedExpression, Expression) => Expression =
    (fun, nvl, exp) =>
      fun.function.transform {
        case unvl: NamedLambdaVariable if
          nvl.exprId == unvl.exprId =>
          exp
      } ): FunN = {
      val placeHolders = ff.params.collect{ case r: RefExpression => r.index }.toSet
      val nonPlaceHolders = ff.function.arguments.zipWithIndex.foldLeft(ff.function.function.asInstanceOf[LambdaFunction]){ // always a LambdaFunction given you can't partially apply a 0 arg expression
        case (fun, (exp, index) ) =>
          // if it's an nvl leave alone
          if (placeHolders(index))
            fun
          else {
            val nvl = fun.arguments(index)
            fun.copy(function = transformFunction(fun, nvl, exp))
          }
      }
      ff.function.copy(function = nonPlaceHolders.copy( arguments =
        nonPlaceHolders.arguments.zipWithIndex.filter{case (_, index) => placeHolders(index)}.map(_._1) ) )
    }

}

object LambdaFunctions {
  type lambdaFunctions = Map[String, Map[Int, (QLambdaFunction, Expression)]]

  val CallFun = "callFun"
  val Lambda = "_lambda_"
  val PlaceHolder = "_"

  def lambdaArgCount(expression: Expression): Int =
    expression match {
      case LambdaFunction(id, args, _) => args.size
      case e : Expression => 0
    }

  def toFunctionMap(functions: Seq[QLambdaFunction]): lambdaFunctions =
    functions.groupBy(_.name).map{
      pair =>
        val funcs = pair._2
        val res = funcs.map{ f =>
          val expr = f.expr
          lambdaArgCount(expr) -> (f, expr)
        }
        // we want unique argument counts, none of these should have more than one entry
        val counts = res.groupBy(_._1)
        val moreThan1 = counts.collectFirst{ case f if f._2.size > 1 => f}
        moreThan1.foreach { f =>
          qualityException(s"Lambda function ${pair._1} has ${f._2.size} implementations with ${f._1} arguments: ${f._2.map(_._2._1).mkString("\n")}}")
        }

        pair._1 -> res.toMap
    }

  def registerLambdaFunctions(functions: Seq[QLambdaFunction]): Unit = {
    val fs = toFunctionMap(functions)
    val funcReg = SparkSession.getActiveSession.get.sessionState.functionRegistry
    val register = ShimUtils.registerFunction(funcReg) _

    fs.foreach{ case (name, map) =>

      val lf = (expsToUse: Seq[Expression]) => {

        // for each of the sizes at query planning / resolving try to find the correct implementation
        map.get(expsToUse.size).map {
          p =>
            val (numPlaceHolders, hasHofs, actual) = processArgs(expsToUse)

            val replacedFun =
              if (hasHofs) {
                val funrefs = actual.zipWithIndex.filter(p => p._1.isInstanceOf[FunN]
                  || p._1.isInstanceOf[FunForward])
                val rfun =
                  funrefs.foldLeft(p._2.asInstanceOf[LambdaFunction]) { // this will be the case if we have params
                    case (fun: LambdaFunction, (funRef, index)) =>
                      val nvl = fun.arguments(index)
                      val isTheLambdaVar = arg1IsTheNvl(nvl) _

                      val newF = fun.function.transform {
                        case unF: UnresolvedFunction
                          if toName(unF) == CallFun && isTheLambdaVar(unF) =>
                          processCallFun(funRef, unF)

                        // the case where we just pass it through
                        case unvl: UnresolvedNamedLambdaVariable if unvl.name == nvl.name =>
                          passFunXThrough(funRef)

                        // the case of droppins
                        case unF: UnresolvedFunction
                          if toName(unF) == Lambda && isTheLambdaVar(unF) =>
                          processLambdaCall(funRef)

                        // the case of trying to call an applyFun
                        case unF: UnresolvedFunction
                          if toName(unF) == CallFun && arg1IsCallFun(unF) =>
                          processCallFunOnFun(funRef, unF)
                      }
                      fun.copy(function = newF)
                  }
                val funrefidx = funrefs.map(_._2).toSet
                // strip out the funrefs
                rfun.copy(arguments = rfun.arguments.zipWithIndex.filterNot(p => funrefidx(p._2)).map(_._1))
              } else
                p._2

            val replacedArgs =
              actual.filterNot(p => p.isInstanceOf[FunN] || p.isInstanceOf[FunForward]).
                zipWithIndex.collect{ // make sure they are indexed even if the are never used -
                // needed for user partial functions to create a ref that are 1:1
                  case (r: RefExpression, index) => r.copy(index = index)
                  case (e, _) => e
                }

            val actualFun = FunN(replacedArgs, replacedFun, Some(name))
            if (numPlaceHolders == 0 || numPlaceHolders == expsToUse.size)
              actualFun
            else
            // wrap it for partials
              wrapFunN(replacedArgs, actualFun)
        }.getOrElse(qualityException(s"${expsToUse.size} arguments requested for $name but no implementation with this argument count exists"))
      }

      register(name, lf)
    }
  }

  /**
   * Ensure the indexes are correct against these arguments
   * @param replacedArgs
   * @param actualFun
   * @return
   */
  def wrapFunN(replacedArgs: Seq[Expression], actualFun: FunN): FunForward = {
    FunForward(replacedArgs.zipWithIndex.filter(_._1.isInstanceOf[RefExpression]).
      map(p => p._1.asInstanceOf[RefExpression].copy(index = p._2)) :+ actualFun)
  }

  private def processArgs(expsToUse: Seq[Expression]) = {
    var numPlaceHolders = 0
    var hasHofs = false

    val actual = expsToUse.map {
      case PlaceHolderExpression(dataType, nullable) =>
        numPlaceHolders += 1
        RefExpression(dataType, nullable)
      case funF: FunForward =>
        // 1:1 params forward
        hasHofs = true
        funF
      case fun: FunN =>
        // auto wrap in FunRef
        hasHofs = true
        fun
      case e => e
    }
    (numPlaceHolders, hasHofs, actual)
  }

  private def arg1IsTheNvl(nvl: NamedExpression)( unF: UnresolvedFunction) =
    if (ShimUtils.arguments(unF)(0).isInstanceOf[UnresolvedNamedLambdaVariable]) {
      val unvl = ShimUtils.arguments(unF)(0).asInstanceOf[UnresolvedNamedLambdaVariable]
      if (unvl.name == nvl.name) // not great for nesting perhaps
        true
      else
        false
    } else
      false

  private def arg1IsCallFun( unF: UnresolvedFunction ) =
    ShimUtils.arguments(unF)(0) match {
      case f: UnresolvedFunction if toName(f) == CallFun => true
      case _ => false
    }

  private def processCallFun(funRef: Expression, unF: UnresolvedFunction) = {
    // replace args, drop the function
    val params = ShimUtils.arguments(unF).drop(1)
    funRef match {
      case fun: FunN =>
        fun.copy(arguments = params)
      case ff: FunForward =>
        // partially applied, so we need to reapply
        ff.function.copy(arguments = params.zip(ff.params).foldLeft(ff.function.arguments) {
          case (args, (param, ref: RefExpression)) =>
            args.updated(ref.index, param)
        })
    }
  }

  def processTopCallFun(fun: FunN, l: LambdaFunction, ff: FunForward, args: Seq[Expression]) = {
    val argmap = ff.params.zip(args).map { case (ref: RefExpression, e: Expression) => ref.index -> e }.toMap
    // fun is lambda with a funforward in it.  The args are correct
    val tmp = ff.function.copy(arguments = ff.function.arguments.collect {
      case ref: RefExpression =>
        argmap(ref.index)
      case arg => arg
    })
    val res = fun.copy(function = l.copy(function = tmp))
    res
  }

  private def processCallFunOnFun(funRef: Expression, unF: UnresolvedFunction) = {
    // replace args, drop the function
    val Upfun = ShimUtils.arguments(unF)(0)
    val pfun = processApplyFun(funRef, Upfun.asInstanceOf[UnresolvedFunction])

    val tmp = applyFunN(pfun)
    // replace the callFun params
    val args = ShimUtils.arguments(unF).drop(1)
    val res = tmp.copy( arguments = tmp.arguments.map{
      case ref: RefExpression => args(ref.index)
      case e => e
    })

    res
  }

  private def processApplyFun(funRef: Expression, unF: UnresolvedFunction) = {
    // unlike the above we need to search for these arguments and treat them as applications
    val args = ShimUtils.arguments(unF)

    // replace args, drop the function
    val params = args.drop(1).zipWithIndex.map {
      case (unf: UnresolvedFunction, index) if toName(unf) == PlaceHolder =>
        val unfParams = ShimUtils.arguments(unf)
        unfParams match {
          case Seq(Literal(str: UTF8String, StringType)) =>
            RefExpression(DataType.fromDDL(str.toString), true, index)
          case Seq(Literal(str: UTF8String, StringType), Literal(bol: Boolean, BooleanType) ) =>
            RefExpression(DataType.fromDDL(str.toString), bol, index)
          case _ =>
            RefExpression(LongType, true, index) // default to nullable
        }
      case (e, _) => e
    }

    funRef match {
      case ff: FunForward =>
        ff.copy(params :+ ff.function.copy(arguments = params))
      case fun: FunN =>
        // wrap it up
        FunForward(params :+ fun.copy(arguments = params))
    }
  }

  private def processLambdaCall(funRef: Expression) =
    funRef match {
      case fun: FunN =>
        // 1:1
        fun.function
      case ff: FunForward =>
        // partially applied, so we need to reapply
        val f = FunCall(ff)
        f
    }

  private def passFunXThrough(funRef: Expression) = {
    funRef match {
      case fun: FunN =>
        fun
      case ff: FunForward =>
        // assume pass again
        ff
    }
  }
}


/**
 * Only used with Lambda placeholders, defaults to allowing nullable values
 * @param dataType
 */
case class PlaceHolderExpression(dataType: DataType, nullable: Boolean = true) extends LeafExpression with Unevaluable {
}
