package com.sparkutils.quality.impl.extension

import com.sparkutils.quality.impl.extension.QualitySparkExtension.disabledOptimiserRules
import org.apache.spark.sql.ClassicQualitySparkUtils
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
object FunNRewrite extends FunNRewriteBase {

  override def className = "com.sparkutils.quality.impl.extension.FunNRewrite"

  lazy val disabled: Boolean = shouldBeDisabled

}

trait FunNRewriteBase extends Rule[LogicalPlan] {
  def className: String

  def shouldBeDisabled: Boolean = {
    val (all, disabledRules) = disabledOptimiserRules()
    if (all)
      true
    else
      disabledRules.contains(className)
  }

  def disabled: Boolean

  def funNHandled(f: FunN): Boolean = f.name.isDefined && compilationHandlers.contains(f.name.get)

  override def apply(plan: LogicalPlan): LogicalPlan =
    if (disabled)
      plan
    else
      ClassicQualitySparkUtils.funNRewrite(plan, {
        case f: FunN if !f.usedAsLambda &&
          // if a direct child is a rewrite HoF then we shouldn't disable compilation by ripping it out (#83)
          !f.children.exists( t => t.collect {
            // full class type for HoF
            case e if compilationHandlers.contains(e.getClass.getName) => true
            // specific FunN name
            case f: FunN if funNHandled(f) => true
          }.nonEmpty )
          &&
          // if this FunN should be handled then we shouldn't rewrite
          !(funNHandled(f)) =>
          val pairs = f.elementVars.zip(f.arguments).toMap
          val r =
            f.function.asInstanceOf[LambdaFunction].function.transform{
              case e: NamedLambdaVariable if pairs.contains(e) => pairs(e)
            }
          r
      })
}
