package org.apache.spark.sql

import com.sparkutils.quality.impl.util.DebugTime.debugTime
import com.sparkutils.quality.impl.util.Params.formatParams
import com.sparkutils.quality.impl.util.{PassThrough, PassThroughCompileEvals}
import com.sparkutils.quality.impl.{RuleEngineRunnerBase, RuleFolderRunnerBase, RuleRunnerBase}
import com.sparkutils.shim.expressions.PredicateHelperPlus
import org.apache.spark.sql.QualityStructFunctions.UpdateFields
import org.apache.spark.sql.ShimUtils.{column, toSQLExpr, toSQLType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{Analyzer, ResolveCatalogs, ResolveHigherOrderFunctions, ResolveInlineTables, ResolveLambdaVariables, ResolvePartitionSpec, ResolveTableValuedFunctions, ResolveTimeZone, ResolveUnion, Resolver, TimeWindowing, TypeCheckResult, TypeCoercion, UnresolvedAttribute, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, CodegenFallback, ExprCode, ExprUtils, GenerateMutableProjection}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, BindReferences, CreateNamedStruct, EqualNullSafe, Expression, ExpressionSet, ExtractValue, GetStructField, If, InterpretedMutableProjection, IsNull, LeafExpression, Literal, Projection, UnaryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.optimizer.{BooleanSimplification, CollapseProject, CombineConcats, CombineTypedFilters, ConstantFolding, ConstantPropagation, EliminateMapObjects, EliminateSerialization, FoldablePropagation, LikeSimplification, NormalizeFloatingNumbers, NullPropagation, ObjectSerializerPruning, OptimizeIn, OptimizeUpdateFields, ReassignLambdaVariableID, RemoveDispensableExpressions, RemoveNoopOperators, RemoveRedundantAliases, ReorderAssociativeOperator, ReplaceNullWithFalseInPredicate, ReplaceUpdateFieldsExpression, SimplifyBinaryComparison, SimplifyCaseConversionExpressions, SimplifyCasts, SimplifyConditionals, SimplifyExtractValueOps, UnwrapCastInBinaryComparison}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project, UnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.util.Utils

import scala.collection.mutable.ArrayBuffer

/**
 * Set of utilities to reach in to private functions
 */
object QualitySparkUtils {

  /**
   * Spark >3.1 supports the very useful getLocalInputVariableValues, 2.4 needs the previous approach
   *
   * @param i
   * @param ctx
   * @return (parameters for function decleration, parmaters for calling, code that must be before fungroup)
   */
  def genParams(ctx: CodegenContext, child: Expression): (String, String, String) = {
    val (a, b) = CodeGenerator.getLocalInputVariableValues(ctx, child, ExprUtils.currentSubExprState(ctx))

    val p = formatParams( ctx, a.toSeq )

    (p._1, p._2, b.map(_.code.code).mkString("\n"))
  }

  def funNRewrite(plan: LogicalPlan, expressionToExpression: PartialFunction[Expression, Expression]): LogicalPlan =
    plan // no-op on <3.2

  type DatasetBase[F] = org.apache.spark.sql.Dataset[F]

  case class EvaluableExpressions(plan: LogicalPlan) extends PredicateHelperPlus {
    def expressions: Seq[Expression] = plan match {
      case p: Project => p.expressions.map{
        e => findRootExpression(e, plan).getOrElse(e)
      }
      case _ => plan.expressions
    }
  }

  lazy val optimizerBatches = Seq(
    CollapseProject,
    NullPropagation,
    ConstantPropagation,
    FoldablePropagation,
    OptimizeIn,
    ConstantFolding,
    ReorderAssociativeOperator,
    LikeSimplification,
    BooleanSimplification,
    SimplifyConditionals,
    RemoveDispensableExpressions,
    SimplifyBinaryComparison,
    ReplaceNullWithFalseInPredicate,
    SimplifyCasts,
    SimplifyCaseConversionExpressions,
    EliminateSerialization,
    RemoveRedundantAliases,
    UnwrapCastInBinaryComparison,
    RemoveNoopOperators,
    OptimizeUpdateFields,
    SimplifyExtractValueOps,
    CombineConcats,
    EliminateMapObjects,
    CombineTypedFilters,
    ObjectSerializerPruning,
    ReassignLambdaVariableID,
    NormalizeFloatingNumbers,
    ReplaceUpdateFieldsExpression
  )

  /**
   * Provides a starting plan for a dataframe, resolves the
   *
   * @param fields input types
   * @param dataFrameF
   * @return
   */
  def resolveExpressions(fields: StructType, dataFrameF: DataFrame => DataFrame): Seq[Expression] = {

    val plan = LocalRelation(fields.fields.map{ field =>
      AttributeReference(field.name, field.dataType, field.nullable, field.metadata)()
    })
    val ag = RowEncoder.apply(fields)

    // this constructor stops execute plan being called too early
    val df = dataFrameF(
      new Dataset[Row](SparkSession.getActiveSession.get.sqlContext, plan, ag)
    )

    // force an optimize
    val aplan =
      (optimizerBatches ++ SparkSession.getActiveSession.get.experimental.extraOptimizations).
        foldLeft(df.queryExecution.analyzed){
          (p, b) =>
            b.apply(p)
        }

    // lookup the actual expressions
    val res = debugTime("find underlying expressions") {
      EvaluableExpressions(aplan).expressions
    }

    // folder introduces multiple projections, these are the ones we explicitly use
    val fres = debugTime("bindReferences") {
      res.map(BindReferences.bindReference(_, df.queryExecution.analyzed.allAttributes, allowFailures = true)).
        map(BindReferences.bindReference(_, aplan.allAttributes, allowFailures = true)).
        map(BindReferences.bindReference(_, plan.output, allowFailures = true))
    }

    fres
  }

  /**
   * Provides a starting plan for a dataframe, resolves the
   *
   * @param encFrom starting data type to encode from
   * @param dataFrameF
   * @return
   */
  def resolveExpressions[T](encFrom: Encoder[T], dataFrameF: DataFrame => DataFrame): Seq[Expression] = {
    val enc = ShimUtils.expressionEncoder(encFrom)

    val plan = LocalRelation(enc.schema.fields.map{ field =>
      AttributeReference(field.name, field.dataType, field.nullable, field.metadata)()
    })

    // this constructor stops execute plan being called too early
    val df = dataFrameF(
      new Dataset[T](SparkSession.getActiveSession.get.sqlContext, plan, enc).toDF()
    )

    // force an optimize
    val aplan =
      (optimizerBatches ++ SparkSession.getActiveSession.get.experimental.extraOptimizations).
        foldLeft(df.queryExecution.analyzed){
          (p, b) =>
            b.apply(p)
        }

    // lookup the actual expressions
    val res = debugTime("find underlying expressions") {
      EvaluableExpressions(aplan).expressions
    }

    // folder introduces multiple projections, these are the ones we explicitly use
    val fres = debugTime("bindReferences") {
      res.map(BindReferences.bindReference(_, df.queryExecution.analyzed.allAttributes, allowFailures = true)).
        map(BindReferences.bindReference(_, aplan.allAttributes, allowFailures = true)).
        map(BindReferences.bindReference(_, plan.output, allowFailures = true))
    }

    fres
  }

  /**
   * Creates a projection from InputRow to InputRow.
   * @param exprs expressions from resolveExpressions, already resolved without
   * @param compile
   * @return typically a mutable projection, callers must ensure partition is set and the target row is provided
   */
  def rowProcessor(exprs: Seq[Expression], compile: Boolean = true): Projection  = {
    if (compile)
      GenerateMutableProjection.generate(exprs, SQLConf.get.subexpressionEliminationEnabled)
    else
      InterpretedMutableProjection.createProjection(exprs)
  }

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
   * @param expr the expression to resolve
   */
  def resolveExpression(dataFrame: DataFrame, expr: Expression): Expression = {

    val sparkSession = SparkSession.getActiveSession.get

    val plan = dataFrame.select("*").logicalPlan // select * needed for toDF's etc. from dataset to force evaluation of the attributes
    val res = debugTime("tryResolveReferences"){
      tryResolveReferences(sparkSession)(expr, plan)
    }

    val fres = debugTime("bindReferences"){BindReferences.bindReference(res, plan.allAttributes)}

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
            throw new TreeNodeException(curPlan, message, null)
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


  def resolution(analyzer: Analyzer, sparkSession: SparkSession) = {
    val conf = sparkSession.sqlContext.conf
    val fixedPoint = new Strategy(
      conf.analyzerMaxIterations,
      errorOnExceed = true,
      maxIterationsSetting = SQLConf.ANALYZER_MAX_ITERATIONS.key)

    import analyzer._

    val v1SessionCatalog: SessionCatalog = catalogManager.v1SessionCatalog

    Batch("Resolution", fixedPoint,
      ResolveTableValuedFunctions ::
        ResolveNamespace(catalogManager) ::
        new ResolveCatalogs(catalogManager) ::
        ResolveUserSpecifiedColumns ::
        ResolveInsertInto ::
        ResolveRelations ::
        ResolveTables ::
        ResolvePartitionSpec ::
        AddMetadataColumns ::
        ResolveReferences ::
        ResolveDeserializer ::
        ResolveNewInstance ::
        ResolveUpCast ::
        ResolveGroupingAnalytics ::
        ResolvePivot ::
        ResolveOrdinalInOrderByAndGroupBy ::
        ResolveAggAliasInGroupBy ::
        ResolveMissingReferences ::
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
        ResolveInlineTables ::
        ResolveHigherOrderFunctions(v1SessionCatalog) ::
        ResolveLambdaVariables ::
        ResolveTimeZone ::
        ResolveRandomSeed ::
        ResolveBinaryArithmetic ::
        ResolveUnion ::
        TypeCoercion.typeCoercionRules
          : _*)
      }

  // below based on approach from delta / discussed with Alex to use a Project, LeafNode should be fine
  protected def tryResolveReferences(
                                  sparkSession: SparkSession)(
                                  expr: Expression,
                                  child: LogicalPlan): Expression = {
    val analyzer = sparkSession.sessionState.analyzer

    def forExpr(expr: Expression) = {
      val newPlan = FakePlan(expr, child)
      //analyzer.execute(newPlan)
      execute(newPlan, resolution(analyzer, sparkSession))
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
      case r: RuleEngineRunnerBase[_] if r.child.isInstanceOf[PassThrough] =>
        val nexprs = r.child.children.map(forExpr)
        r.withNewChildren(Seq(r.child.withNewChildren(nexprs)))
      case r: RuleFolderRunnerBase[_] if r.right.isInstanceOf[PassThrough]  =>
        val nexprs = r.right.children.map(forExpr)
        r.withNewChildren(Seq(r.left, r.right.withNewChildren(nexprs)))
      case r: RuleRunnerBase[_] if r.child.isInstanceOf[PassThrough] =>
        val nexprs = r.child.children.map(forExpr)
        r.withNewChildren(Seq(PassThroughCompileEvals(nexprs)))
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
        case a @ Alias(l: Literal, _) =>
          allConstraints += EqualNullSafe(a.toAttribute, l)
        case a @ Alias(e, _) =>
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
      case UpdateFields(UpdateFields(struct +: fieldOps1) +: fieldOps2) =>
        UpdateFields(struct +: ( fieldOps1 ++ fieldOps2) )
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
}

object QualityStructFunctions {

  /* Below is lifted from 3.4.1 complexTypeCreator.  The 3.1.3 version does not support testSimpleProductionRulesReplaceDebug and ..DebugSet cases due to
  org.apache.spark.sql.catalyst.errors.package$TreeNodeException: The structural integrity of the input plan is broken in org.apache.spark.sql.internal.BaseSessionStateBuilder$$anon$1., tree:
'Project [ArrayBuffer(foldedFields).*]
+- Project [product#1037, account#1036, subcode#1038, foldedFields#1022]
   +- Project [foldedFields#1022, account#1036, product#1037, subcode#1038]
      +- Project [foldedFields#1022, tempFOLDDEBUG#1032, tempFOLDDEBUG#1032.account AS account#1036, tempFOLDDEBUG#1032.product AS product#1037, tempFOLDDEBUG#1032.subcode AS subcode#1038]
         +- Project [foldedFields#1022, element_at(foldedFields#1022.result, -1, false).result AS tempFOLDDEBUG#1032]
            +- Project [foldedFields#1022]
               +- Project [product#1012, account#1013, subcode#1014, RuleFolderRunner(((product#1012 = edt) AND (subcode#1014 = 40)), (product#1012 = eqotc), product#1012 LIKE %fx%, (product#1012 = eqotc), funn(refexpressionlazytype(com.sparkutils.quality.impl.imports.RuleFolderRunnerImports$$Lambda$2253/635236790@6a056d13, true), lambdafunction(update_fields(lambda thecurrent#1023, WithField(subcode, 1234)), lambda thecurrent#1023, false), None, false, false), funn(refexpressionlazytype(com.sparkutils.quality.impl.imports.RuleFolderRunnerImports$$Lambda$2253/635236790@4b11a972, true), lambdafunction(update_fields(lambda thecurrent#1024, WithField(subcode, 6000), WithField(account, concat(lambda thecurrent#1024.account, _fruit))), lambda thecurrent#1024, false), None, false, false), funn(refexpressionlazytype(com.sparkutils.quality.impl.imports.RuleFolderRunnerImports$$Lambda$2253/635236790@77c0457f, true), lambdafunction(update_fields(lambda thecurrent#1025, WithField(account, to)), lambda thecurrent#1025, false), None, false, false), funn(refexpressionlazytype(com.sparkutils.quality.impl.imports.RuleFolderRunnerImports$$Lambda$2253/635236790@1035ec3c, true), lambdafunction(update_fields(lambda thecurrent#1026, WithField(account, from)), lambda thecurrent#1026, false), None, false, false)) AS foldedFields#1022]
                  +- LocalRelation [product#1012, account#1013, subcode#1014]

   */

  /**
   * Represents an operation to be applied to the fields of a struct.
   */
  trait StructFieldsOperation extends Expression {

    val resolver: Resolver = SQLConf.get.resolver

    override def dataType: DataType = throw new IllegalStateException(
      "StructFieldsOperation.dataType should not be called.")

    override def nullable: Boolean = throw new IllegalStateException(
      "StructFieldsOperation.nullable should not be called.")

    /**
     * Returns an updated list of StructFields and Expressions that will ultimately be used
     * as the fields argument for [[StructType]] and as the children argument for
     * [[CreateNamedStruct]] respectively inside of [[UpdateFields]].
     */
    def apply(values: Seq[(StructField, Expression)]): Seq[(StructField, Expression)]
  }

  /**
   * Add or replace a field by name.
   *
   * We extend [[Unevaluable]] here to ensure that [[UpdateFields]] can include it as part of its
   * children, and thereby enable the analyzer to resolve and transform valExpr as necessary.
   */
  case class WithField(name: String, child: Expression)
    extends UnaryExpression with StructFieldsOperation {

    override def foldable: Boolean = false

    override def apply(values: Seq[(StructField, Expression)]): Seq[(StructField, Expression)] = {
      val newFieldExpr = (StructField(name, child.dataType, child.nullable), child)
      val result = ArrayBuffer.empty[(StructField, Expression)]
      var hasMatch = false
      for (existingFieldExpr@(existingField, _) <- values) {
        if (resolver(existingField.name, name)) {
          hasMatch = true
          result += newFieldExpr
        } else {
          result += existingFieldExpr
        }
      }
      if (!hasMatch) result += newFieldExpr
      result.toSeq
    }

    override def prettyName: String = "WithField"

    protected def withNewChildInternal(newChild: Expression): WithField =
      copy(child = newChild)

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = ???
  }

  /**
   * Drop a field by name.
   */
  case class DropField(name: String) extends LeafExpression with StructFieldsOperation {
    override def apply(values: Seq[(StructField, Expression)]): Seq[(StructField, Expression)] =
      values.filterNot { case (field, _) => resolver(field.name, name) }

    override def eval(input: InternalRow): Any = ???

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = ???
  }

  /**
   * Updates fields in a struct.
   */
  case class UpdateFields(children: Seq[Expression])
    extends Expression with CodegenFallback {

    val structExpr = children.head
    val fieldOps: Seq[StructFieldsOperation] = children.drop(1).map(_.asInstanceOf[StructFieldsOperation])

    override def checkInputDataTypes(): TypeCheckResult = {
      val dataType = structExpr.dataType
      if (!dataType.isInstanceOf[StructType]) {
        TypeCheckResult.TypeCheckFailure(message =
          s"UNEXPECTED_INPUT_TYPE, requiredType StructType, inputSql ${toSQLExpr(structExpr)}, inputType ${toSQLType(structExpr.dataType)}"
        )
      } else if (newExprs.isEmpty) {
        TypeCheckResult.TypeCheckFailure(message =
          s"errorSubClass = CANNOT_DROP_ALL_FIELDS"
        )
      } else {
        TypeCheckResult.TypeCheckSuccess
      }
    }

    protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
      copy(children = newChildren)

    override def dataType: StructType = StructType(newFields)

    override def nullable: Boolean = structExpr.nullable

    override def prettyName: String = "update_fields"

    private lazy val newFieldExprs: Seq[(StructField, Expression)] = {
      def getFieldExpr(i: Int): Expression = structExpr match {
        case c: CreateNamedStruct => c.valExprs(i)
        case _ => GetStructField(structExpr, i)
      }

      val fieldsWithIndex = structExpr.dataType.asInstanceOf[StructType].fields.zipWithIndex
      val existingFieldExprs: Seq[(StructField, Expression)] =
        fieldsWithIndex.map { case (field, i) => (field, getFieldExpr(i)) }
      fieldOps.foldLeft(existingFieldExprs)((exprs, op) => op(exprs))
    }

    private lazy val newFields: Seq[StructField] = newFieldExprs.map(_._1)

    lazy val newExprs: Seq[Expression] = newFieldExprs.map(_._2)

    lazy val evalExpr: Expression = {
      val createNamedStructExpr = CreateNamedStruct(newFieldExprs.flatMap {
        case (field, expr) => Seq(Literal(field.name), expr)
      })

      if (structExpr.nullable) {
        If(IsNull(structExpr), Literal(null, dataType), createNamedStructExpr)
      } else {
        createNamedStructExpr
      }
    }

    override def eval(input: InternalRow): Any = evalExpr.eval(input)

  }

  object UpdateFields {
    private def nameParts(fieldName: String): Seq[String] = {
      require(fieldName != null, "fieldName cannot be null")

      if (fieldName.isEmpty) {
        fieldName :: Nil
      } else {
        UnresolvedAttribute.parseAttributeName(fieldName)
        //CatalystSqlParser.parseMultipartIdentifier(fieldName)
      }
    }

    /**
     * Adds/replaces field of `StructType` into `col` expression by name.
     */
    def apply(col: Expression, fieldName: String, expr: Expression): UpdateFields =
      updateFieldsHelper(col, nameParts(fieldName), name => WithField(name, expr))

    /**
     * Drops fields of `StructType` in `col` expression by name.
     */
    def apply(col: Expression, fieldName: String): UpdateFields =
      updateFieldsHelper(col, nameParts(fieldName), name => DropField(name))

    private def updateFieldsHelper(
                                    structExpr: Expression,
                                    namePartsRemaining: Seq[String],
                                    valueFunc: String => StructFieldsOperation): UpdateFields = {
      val fieldName = namePartsRemaining.head
      if (namePartsRemaining.length == 1) {
        UpdateFields(Seq(structExpr, valueFunc(fieldName)))
      } else {
        val newStruct = if (structExpr.resolved) {
          val resolver = SQLConf.get.resolver
          ExtractValue(structExpr, Literal(fieldName), resolver)
        } else {
          UnresolvedExtractValue(structExpr, Literal(fieldName))
        }

        val newValue = updateFieldsHelper(
          structExpr = newStruct,
          namePartsRemaining = namePartsRemaining.tail,
          valueFunc = valueFunc)
        UpdateFields(Seq(structExpr, WithField(fieldName, newValue)))
      }
    }
  }
}