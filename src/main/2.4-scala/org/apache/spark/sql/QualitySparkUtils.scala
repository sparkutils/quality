package org.apache.spark.sql

import com.sparkutils.quality.impl.util.DebugTime.debugTime
import com.sparkutils.quality.impl.util.PassThrough
import com.sparkutils.quality.impl.{RuleEngineRunner, RuleFolderRunner, RuleRunner}
import org.apache.spark.sql.QualityStructFunctions.UpdateFields
import org.apache.spark.sql.ShimUtils.{toSQLExpr, toSQLType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{Analyzer, ResolveHigherOrderFunctions, ResolveInlineTables, ResolveLambdaVariables, ResolveTimeZone, Resolver, TypeCheckResult, TypeCoercion, UnresolvedAttribute, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, BindReferences, CreateNamedStruct, EqualNullSafe, Expression, ExtractValue, GetStructField, If, IsNull, LeafExpression, Literal, UnaryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.{BaseSessionStateBuilder, SQLConf, SessionState}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.util.Utils

import scala.collection.mutable.ArrayBuffer

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
          val endingMsg =
            s", please set 'optimizerMaxIterations' to a larger value."

          val message = s"Max iterations (${iteration - 1}) reached for batch ${batch.name}" +
            s"$endingMsg"
          if (Utils.isTesting) {
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
                       maxIterations: Int
                     )

  case class Batch(name: String, strategy: Strategy, rules: Rule[LogicalPlan]*)

  class Builder(  val isession: SparkSession,
                  val iparentState: Option[SessionState] = None) extends BaseSessionStateBuilder(isession, iparentState) {
    def getCatalog = catalog

    override protected def newBuilder: NewBuilder = ???
  }


  def resolution(analyzer: Analyzer, sparkSession: SparkSession) = {
    val conf = sparkSession.sqlContext.conf
    val fixedPoint = new Strategy(
      conf.optimizerMaxIterations)

    import analyzer._

    val v1SessionCatalog: SessionCatalog = new Builder(sparkSession, Some(sparkSession.sqlContext.sessionState)).getCatalog

    Batch("Resolution", fixedPoint,
      ResolveReferences :: // this is 50s alone for the 1k tests
        //ResolveCreateNamedStruct ::
        ResolveDeserializer ::
        ResolveNewInstance ::
        ResolveUpCast ::
        /* aggr in rules? ResolveGroupingAnalytics ::
        ResolvePivot ::
        ResolveOrdinalInOrderByAndGroupBy ::
        ResolveAggAliasInGroupBy :: */
        ResolveMissingReferences ::
        ExtractGenerator ::
        ResolveGenerate ::
        ResolveFunctions ::
        ResolveAliases ::
        ResolveSubquery ::
        ResolveSubqueryColumnAliases ::
        /* no windows ResolveWindowOrder ::
        ResolveWindowFrame :: */
        ResolveNaturalAndUsingJoin ::
        ResolveOutputRelation ::
        /* no windows ExtractWindowExpressions :: */
        GlobalAggregates ::
        // no aggs ResolveAggregateFunctions ::
        ResolveInlineTables(conf) ::
        ResolveHigherOrderFunctions(v1SessionCatalog) ::
        ResolveLambdaVariables(conf) ::
        ResolveTimeZone(conf) ::
        ResolveRandomSeed ::
        //ResolveBinaryArithmetic ::
        TypeCoercion.typeCoercionRules(conf)  ++  Nil.toSeq :_*)
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

    override lazy val validConstraints: Set[Expression] = mygetAllValidConstraints(Seq(expr))
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