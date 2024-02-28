package org.apache.spark.sql

import com.sparkutils.quality.impl.util.DebugTime.debugTime
import com.sparkutils.quality.impl.util.PassThrough
import com.sparkutils.quality.impl.{RuleEngineRunner, RuleFolderRunner, RuleRunner}
import org.apache.spark.sql.catalyst.analysis.{Analyzer, DeduplicateRelations, ResolveCatalogs, ResolveExpressionsWithNamePlaceholders, ResolveInlineTables, ResolveLambdaVariables, ResolvePartitionSpec, ResolveTimeZone, ResolveUnion, ResolveWithCTE, SessionWindowing, TimeWindowing, TypeCoercion}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, BindReferences, EqualNullSafe, Expression, ExpressionSet, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * Set of utilities to reach in to private functions
 */
object QualitySparkUtils {

  /**
   * Where resolveWith is not possible (e.g. 10.x DBRs) it is disabled here.
   * This is, in the 10.x DBR case, due to the class files for UnaryNode (FakePlan) being radically different and causing an IncompatibleClassChangeError: Implementing class
   * @param orig
   * @return
   */
  def resolveWithOverride(orig: Option[DataFrame]): Option[DataFrame] =
    orig

  /**
   * Resolves expressions against a dataframe, this allows them to be swapped out after name checking - spark cannot then
   * simply optimise the tree so certain things like constant folding etc. won't show up.
   *
   * @param dataFrame resolve's must be against a given dataframe to keep names matching
   * @param expr      the expression to resolve
   */
  def resolveExpression(dataFrame: DataFrame, expr: Expression): Expression = {

    val sparkSession = SparkSession.getActiveSession.get

    val plan = dataFrame.select("*").logicalPlan // select * needed for toDF's etc. from dataset to force evaluation of the attributes
    val res = debugTime("tryResolveReferences") {
      tryResolveReferences(sparkSession)(expr, plan)
    }

    val fres = debugTime("bindReferences") {
      BindReferences.bindReference(res, plan.allAttributes)
    }

    fres
  }

  def execute(logicalPlan: LogicalPlan, batch: Batch) = {
    var iteration = 1
    var curPlan = logicalPlan
    var lastPlan = logicalPlan

    var start = System.currentTimeMillis


    var continue = true
    val analyzer = SparkSession.getActiveSession.get.sessionState.analyzer

    // Run until fix point (or the max number of iterations as specified in the strategy.
    while (continue) {
      curPlan = batch.rules.foldLeft(curPlan) {
        case (plan, rule) =>
          val startTime = System.nanoTime()
          val result = rule(plan)

          result
      }
      iteration += 1
      if (iteration > batch.strategy.maxIterations) {
        // Only log if this is a rule that is supposed to run more than once.
        if (iteration != 2) {
          val endingMsg = if (batch.strategy.maxIterationsSetting == null) {
            "."
          } else {
            s", please set '${batch.strategy.maxIterationsSetting}' to a larger value."
          }
          val message = s"Max iterations (${iteration - 1}) reached for batch ${batch.name}" +
            s"$endingMsg"
          if (Utils.isTesting || batch.strategy.errorOnExceed) {
            throw new Exception(message)
          } else {
          }
        }
        continue = false
      }

      if (curPlan.fastEquals(lastPlan)) {
        continue = false
      }
      lastPlan = curPlan
    }
    var stop = System.currentTimeMillis
    //println(s"did $iteration iterations to execute the plan in ${stop-start}ms")
    curPlan
  }

  case class Strategy(
                       maxIterations: Int, errorOnExceed: Boolean = false, maxIterationsSetting: String = null
                     )

  case class Batch(name: String, strategy: Strategy, rules: Rule[LogicalPlan]*)


  def resolution(analyzer: Analyzer, sparkSession: SparkSession, plan: LogicalPlan) = {
    val conf = sparkSession.sqlContext.conf
    val fixedPoint = new Strategy(
      conf.analyzerMaxIterations,
      errorOnExceed = true,
      maxIterationsSetting = SQLConf.ANALYZER_MAX_ITERATIONS.key)

    import analyzer._

    Batch("Resolution", fixedPoint,
//      ResolveNamespace(catalogManager) :: works on 11.0 and 11.1, fails on all other 11.x
        new ResolveCatalogs(catalogManager) ::
        ResolveInsertInto ::
        ResolveRelations ::
//        ResolveTables ::
        ResolvePartitionSpec ::
//        ResolveAlterTableCommands ::
        AddMetadataColumns ::
        DeduplicateRelations ::
        new ResolveReferences(catalogManager) ::
        ResolveExpressionsWithNamePlaceholders ::
        ResolveDeserializer ::
        ResolveNewInstance ::
        ResolveUpCast ::
        ResolveGroupingAnalytics ::
        ResolvePivot ::
        ResolveOrdinalInOrderByAndGroupBy ::
        //ResolveAggAliasInGroupBy ::
        //ResolveMissingReferences ::
        ExtractGenerator ::
        ResolveGenerate ::
        ResolveFunctions ::
        ResolveAliases ::
        ResolveSubquery ::
        ResolveSubqueryColumnAliases ::
        ResolveWindowOrder ::
        ResolveWindowFrame ::
        ResolveNaturalAndUsingJoin ::
        ResolveOutputRelation ::
        ExtractWindowExpressions ::
        GlobalAggregates ::
        ResolveAggregateFunctions ::
        TimeWindowing ::
        SessionWindowing ::
        ResolveInlineTables ::
        // ResolveHigherOrderFunctions(catalogManager) ::
        ResolveLambdaVariables ::
        ResolveTimeZone ::
        ResolveRandomSeed ::
        ResolveBinaryArithmetic ::
        ResolveUnion ::
        TypeCoercion.typeCoercionRules ++
          Seq(ResolveWithCTE): _*)
  }

  // below based on approach from delta / discussed with Alex to use a Project, LeafNode should be fine
  protected def tryResolveReferences(
                                      sparkSession: SparkSession)(
                                      expr: Expression,
                                      child: LogicalPlan): Expression =  {
       val analyzer = sparkSession.sessionState.analyzer

       def forExpr(expr: Expression) = {
         val newPlan = FakePlan(expr, child)
         //analyzer.execute(newPlan)
         execute(newPlan, resolution(analyzer, sparkSession, newPlan))
         match {
           case FakePlan(resolvedExpr, _) =>
             // Return even if it did not successfully resolve
             resolvedExpr
           case _ =>
             // This is unexpected
             throw new Exception(
               s"Could not resolve expression $expr with child $child}")
         }
       }
       // special case as it's faster to do individual items it seems, 36816ms vs 48974ms
       expr match {
         case r @ RuleEngineRunner(ruleSuite, PassThrough( expressions ), realType, compileEvals, debugMode, func, group, forceRunnerEval, expressionOffsets, forceTriggerEval) =>
           val nexprs = expressions.map(forExpr)
           RuleEngineRunner(ruleSuite, PassThrough( nexprs ), realType, compileEvals, debugMode, func, group, forceRunnerEval, expressionOffsets, forceTriggerEval)
         case r @ RuleFolderRunner(ruleSuite, left, PassThrough( expressions ), resultDataType, compileEvals, debugMode, variablesPerFunc,
          variableFuncGroup, forceRunnerEval, expressionOffsets, dataRef, forceTriggerEval) =>
           val nexprs = expressions.map(forExpr)
           RuleFolderRunner(ruleSuite, left, PassThrough( nexprs ), resultDataType, compileEvals, debugMode, variablesPerFunc,
             variableFuncGroup, forceRunnerEval, expressionOffsets, dataRef, forceTriggerEval)
         case r @ RuleRunner(ruleSuite, PassThrough( expressions ), compileEvals, func, group, forceRunnerEval) =>
           val nexprs = expressions.map(forExpr)
           RuleRunner(ruleSuite, PassThrough( nexprs ), compileEvals, func, group, forceRunnerEval)
         case _ => forExpr(expr)
       }
     }

     case class FakePlan(expr: Expression, child: LogicalPlan)
       extends UnaryNode {

       override def output: Seq[Attribute] = child.allAttributes.attrs

       override def maxRows: Option[Long] = Some(1)

       protected def mygetAllValidConstraints(projectList: Seq[Expression]): Set[Expression] = {
         var allConstraints = Set.empty[Expression]
         projectList.foreach {
           case a@Alias(l: Literal, _) =>
             allConstraints += EqualNullSafe(a.toAttribute, l)
           case a@Alias(e, _) =>
             // For every alias in `projectList`, replace the reference in constraints by its attribute.
             allConstraints ++= allConstraints.map(_ transform {
               case expr: Expression if expr.semanticEquals(e) =>
                 a.toAttribute
             })
             allConstraints += EqualNullSafe(e, a.toAttribute)
           case _ => // Don't change.
         }

         allConstraints
       }

       override lazy val validConstraints: ExpressionSet = ExpressionSet(mygetAllValidConstraints(Seq(expr)))

       protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = copy(child = newChild)
     }
}
