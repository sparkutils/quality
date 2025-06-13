package com.sparkutils.quality.impl.extension

import org.apache.spark.sql.QualitySparkUtils
import org.apache.spark.sql.catalyst.expressions.{Expression, LambdaFunction, NamedLambdaVariable}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.qualityFunctions.FunN
import org.apache.spark.sql.qualityFunctions.LambdaCompilationUtils.compilationHandlers

/**
 * Rewrites FunN's when not used as LambdaFunctions (e.g. a parent HigherOrderFunction or HigherOrderFunctionLike).
 * The application is a simple replacement of the resolved input expression with all occurrences of the
 * matching LambdaVariable.
 *
 * This happens early enough to allow for user functions to be subexpr eliminated (as of Spark 4 not possible
 * with either CodegenFallback or an expression containing any LambdaVariables).
 *
 * The use of USED_AS_LAMBDA and any Spark HoF with a registered LambdaCompilationHandler will keep the expression
 * as a lambda.  The LambdaCompilationHandler case is a trade-off between overhead of tree copies/compilation in
 * processors, the underlying hof eval vs compile and sub expression elimination.
 *
 * IMPORTANT: This only works on 3.2 and above as it introduced transformDownWithPruning
 */
object FunNRewrite extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan =
    QualitySparkUtils.funNRewrite(plan, {
      case f: FunN if !f.usedAsLambda &&
        // if a direct child is a rewrite HoF then we shouldn't disable compilation by ripping it out (#83)
        !f.children.exists( t => t.collect{
          case e => compilationHandlers.contains(e.getClass.getName)
        }.nonEmpty ) =>
        val pairs = f.elementVars.zip(f.arguments).toMap
        val r =
          f.function.asInstanceOf[LambdaFunction].function.transform{
            case e: NamedLambdaVariable if pairs.contains(e) => pairs(e)
          }
        r
    })
}
