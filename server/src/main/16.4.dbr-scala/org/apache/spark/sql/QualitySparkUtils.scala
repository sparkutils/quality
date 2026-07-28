package org.apache.spark.sql

import org.apache.spark.sql.ShimUtils.{column, expressionEncoder}
import com.sparkutils.quality.impl.util.DebugTime.debugTime
import com.sparkutils.quality.impl.util.Params.formatParams
import com.sparkutils.quality.impl.util.{EmbeddedTypeCorrection, ParameterInformation, PassThrough, PassThroughCompileEvals}
import com.sparkutils.quality.impl.{LambdaFunction, RuleEngineRunnerBase, RuleFolderRunnerBase, RuleRunnerBase}
import com.sparkutils.shim.expressions.HigherOrderFunctionLike
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{Analyzer, DeduplicateRelations, ResolveCatalogs, ResolveExpressionsWithNamePlaceholders, ResolveInlineTables, ResolveLambdaVariables, ResolvePartitionSpec, ResolveTimeZone, ResolveUnion, ResolveWithCTE, SessionWindowing, TimeWindowing, TypeCoercion}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, EmptyBlock, ExprCode, ExprValue, JavaCode, ShimExprUtils, SubExprEliminationState, VariableValue}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, BindReferences, BoundReference, EqualNullSafe, Expression, ExpressionEquals, ExpressionSet, HigherOrderFunction, Literal, Projection, UpdateFields}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.aggregate.{ScalaAggregator, TypedAggregateExpression}
import org.apache.spark.sql.expressions.{Aggregator, UserDefinedAggregator}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.qualityFunctions.{FunN, LambdaFunctions}
import org.apache.spark.util.Utils

import scala.collection.mutable

/**
 * Set of utilities to reach in to private functions
 */
object ClassicQualitySparkUtils {

  /**
   * Spark >3.1 supports the very useful getLocalInputVariableValues, 2.4 needs the previous approach
   *
   * @param i
   * @param ctx
   * @return (parameters for function declaration, parameters for calling, code that must be before fungroup)
   */
  def genParams(ctx: CodegenContext, child: Expression, additional: Seq[(VariableValue, Boolean)] = Seq.empty): ParameterInformation = {
    val (a, b) = CodeGenerator.getLocalInputVariableValues(ctx, child, ShimExprUtils.currentSubExprState(ctx))

    val p = formatParams(ctx, a.toSeq, additional)

    p.copy(pushToTop = b.map(_.code.code).mkString("\n"))
  }

  def funNRewrite(plan: LogicalPlan, expressionToExpression: PartialFunction[Expression, Expression]): LogicalPlan =
    plan.transformAllExpressionsWithPruning {
      // if it's an actual lambda (e.g. folder) we should not expand it for now
      case f: FunN if f.usedAsLambda || f.children.exists { // immediate children check
        case f: FunN =>
          f.usedAsLambda // false is fine
        case _: HigherOrderFunction => true
        case _: HigherOrderFunctionLike => true
        case _ => false
      } => false // if it's an actual lambda (e.g. folder) we should not expand it for now
      case f: FunN if !f.usedAsLambda => true // otherwise assume it's fine
      case _: HigherOrderFunction => false
      case _: HigherOrderFunctionLike => false
      case _ => true
    }(expressionToExpression)

  type DatasetBase[F] = org.apache.spark.sql.Dataset[F]

  /**
   * Provides a starting plan for a dataframe, resolves the
   *
   * @param fields input types
   * @param dataFrameF
   * @return
   */
  def resolveExpressions(fields: StructType, dataFrameF: DataFrame => DataFrame): Seq[Expression] =
    throw new Exception("Not supported on Databricks runtimes")

  /**
   * Provides a starting plan for a dataframe, resolves the
   *
   * @param encFrom starting data type to encode from
   * @param dataFrameF
   * @return
   */
  def resolveExpressions[T, R: Encoder](encFrom: Encoder[T], embeddedTypeCorrection: EmbeddedTypeCorrection, dataFrameF: DataFrame => DataFrame): (Seq[Expression], Expression) =
    throw new Exception("Not supported on Databricks runtimes")

  /**
   * Creates a projection from InputRow to InputRow.
   * @param exprs expressions from resolveExpressions, already resolved without
   * @param compile
   * @return typically a mutable projection, callers must ensure partition is set and the target row is provided
   */
  def rowProcessor(exprs: Seq[Expression], compile: Boolean = true): Projection =
    throw new Exception("Not supported on Databricks runtimes")

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
        case r: RuleEngineRunnerBase[_] if r.children.head.isInstanceOf[PassThrough] =>
          val nexprs = r.children.map(c => c.withNewChildren(Seq(forExpr(c.children.head))))
          r.withNewChildren(nexprs)
        case r: RuleFolderRunnerBase[_] if r.children(1).isInstanceOf[PassThrough] =>
          val starter = r.children.head
          val nexprs = r.children.tail.map(c => c.withNewChildren(Seq(forExpr(c.children.head))))
          r.withNewChildren(starter +: nexprs)
        case r: RuleRunnerBase[_] if r.children.head.isInstanceOf[PassThrough] =>
          val nexprs = r.children.map(c => c.withNewChildren(Seq(forExpr(c.children.head))))
          r.withNewChildren(nexprs)
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
  /**
   * Adds fields, in order, for each field path it's paired transformation is applied to the update column
   *
   * @param update
   * @param transformations
   * @return a new copy of update with the changes applied
   */
  def update_field(update: Column, transformations: (String, Column)*): Column =
    column(
      transformFields{
        transformations.foldRight(update.expr) {
          case ((path, col), origin) =>
            UpdateFields.apply(origin, path, col.expr)
        }
      }
    )

  protected def transformFields(exp: Expression): Expression =
    exp.transform { // simplify, normally done in optimizer UpdateFields
      case UpdateFields(UpdateFields(struct, fieldOps1), fieldOps2) =>
        UpdateFields(struct, fieldOps1 ++ fieldOps2 )
    }

  /**
   * Drops a field from a structure
   * @param update
   * @param fieldNames may be nested
   * @return
   */
  def drop_field(update: Column, fieldNames: String*): Column =
    column(
      transformFields{
        fieldNames.foldRight(update.expr) {
          case (fieldName, origin) =>
            UpdateFields.apply(origin, fieldName)
        }
      }
    )

  def aggregator[I: Encoder, B, O](agg: Aggregator[I,B,O], exps: Seq[Expression]) =
    ScalaAggregator(UserDefinedAggregator(agg, implicitly[Encoder[I]]), exps).toAggregateExpression()

  object ScalaAggregator {
    def apply[IN, BUF, OUT](
                             uda: UserDefinedAggregator[IN, BUF, OUT],
                             children: Seq[Expression]): ScalaAggregator[IN, BUF, OUT] = {
      new ScalaAggregator(
        children = children,
        agg = uda.aggregator,
        inputEncoder = expressionEncoder(uda.inputEncoder),
        bufferEncoder = expressionEncoder(uda.aggregator.bufferEncoder),
        nullable = uda.nullable,
        isDeterministic = uda.deterministic)
    }
  }
}
