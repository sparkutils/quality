package com.sparkutils.quality.impl

import com.sparkutils.quality.QualityException
import com.sparkutils.quality.sparkless.impl.DecoderOpEncoderProjection
import com.sparkutils.quality.sparkless.impl.Processors.{NO_QUERY_PLANS, isCopyNeeded}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{Encoder, QualitySparkUtils, ShimUtils}
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.NoOp
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{ExprUtils, _}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

//import scala.collection.immutable.{Map, Seq}

trait NullableType[T] {
  def isNullable(t: T): Boolean
  def global(name: String, t: T): GlobalValue

  def javaType(t: T): String
}

object NullableType {

  implicit val ExprNullableType = new NullableType[Expression] {

    override def isNullable(t: Expression): Boolean = t.nullable

    override def global(name: String, t: Expression): GlobalValue =
      JavaCode.global(name, t.dataType)

    override def javaType(t: Expression): String = JavaCode.javaType(t.dataType).code
  }

  implicit val ExprCodeNullableType = new NullableType[ExprCode] {
    override def isNullable(t: ExprCode): Boolean =
      t.isNull match {
        case FalseLiteral => false
        case _ => true
      }

    override def global(name: String, t: ExprCode): GlobalValue =
      JavaCode.global(name, t.value.javaType)

    override def javaType(t: ExprCode): String = JavaCode.javaType(t.value.javaType).code
  }
}

/**
 * CODE is based on MutableProjection, but uses currentVars and wholestage elimination and generates a transformation between
 * two encoders over a middle operation.
 * The generated class itself can create new instances directly, unlike Spark projections that need to go through the
 * codegen source generation cycle, only the compilation is cached.
 *
 * If the expression tree contains stateful expressions with codegenfallback the code must be regenerated against a
 * fresh tree.
 */
object GenerateDecoderOpEncoderVarProjection extends CodeGenerator[Seq[Expression], DecoderOpEncoderProjection[_,_]] {

  // $COVERAGE-OFF$
  protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer.execute)

  protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    bindReferences(in, inputSchema)
  // $COVERAGE-ON$

  // $COVERAGE-OFF$
  protected def create(expressions: Seq[Expression]): DecoderOpEncoderProjection[_,_] = ???
  // $COVERAGE-ON$

  // only for subexpresssion elimination, the tree is cleaned for statefuls on each new invocation, just not the first
  // one and freshCopyIfContainsStatefulExpression is also >3.4.1 only
  def generateExpressions(ctx: CodegenContext, expressions: Seq[Expression], subExprState: Map[ExpressionEquals,
    SubExprEliminationState]): Seq[ExprCode] =
    ctx.withSubExprEliminationExprs(subExprState)( expressions.toIndexedSeq.map(e => e.genCode(ctx) ) )

  def projections(ctx: CodegenContext, expressions: Seq[Expression], mutableRow: String,
                  subExprState: Map[ExpressionEquals, SubExprEliminationState] = Map.empty) = {
    val validExpr = expressions.zipWithIndex.filter {
      case (NoOp, _) => false
      case _ => true
    }

    val exprVals =
      generateExpressions(ctx, validExpr.map(_._1), subExprState).toIndexedSeq

    // 4-tuples: (code for projection, isNull variable name, value variable name, column index)
    val projectionCodes: Seq[(ExprCode, String, String)] = validExpr.zip(exprVals).map {
      case ((e, i), ev) =>
        val (code: String, exprCode: ExprCode) = topLevelProxy(ctx, e, ev)
        val update = CodeGenerator.updateColumn(
          mutableRow,
          e.dataType,
          i,
          exprCode,
          e.nullable)
        (exprCode, code, update)
    }
    projectionCodes
  }

  def rewriteToGlobal: (ExprCode, GlobalValue, () => String) => ExprCode = (expr, value, nullFun) => {
    val newName = value.value

    (expr.isNull, expr.value) match {
      case (_, GlobalValue(_, _)) => expr
      case (FalseLiteral, v@VariableValue(_, typ)) =>
        ExprCode(code = code"${expr.code.code.replaceAll(ExprUtils.stripBrackets(v), newName)}",
          isNull = FalseLiteral, value = GlobalValue(newName, typ)
      )
      case (n@VariableValue(_, ntype), v@VariableValue(_, typ)) =>
        val newIsNull = nullFun()
        ExprCode(code = code"${expr.code.code.replaceAll(
          ExprUtils.stripBrackets(v), newName).replaceAll(ExprUtils.stripBrackets(n), newIsNull)}",
          isNull = GlobalValue(newIsNull, ntype), value = GlobalValue(newName, typ)
      )
    }

  }

  // default spark one does not wrap any variables in globals so splitting etc. doesn't compile
  def evaluateSubExprEliminationState(ctx: CodegenContext, subExprStates: Iterable[SubExprEliminationState], proxy: Boolean = false): String = {
    val code = new StringBuilder()

    subExprStates.foreach { state =>
      val currentCode = evaluateSubExprEliminationState(ctx, state.children) + "\n" + (
          if (proxy)
            topLevelProxy(ctx, state.eval, state.eval, useFreshName = false, valueName = state.eval.value.code,
              isNullName = state.eval.isNull.code, rewriteCode = rewriteToGlobal)._1
          else
            state.eval.code
        )
      code.append(currentCode + "\n")
      state.eval.code = EmptyBlock
    }

    code.toString()
  }

  private def subExprElim(ctx: CodegenContext, exprsToUse: Seq[Expression], proxy: Boolean = false) = {

    val subExprs = ctx.subexpressionEliminationForWholeStageCodegen(exprsToUse)
    (evaluateSubExprEliminationState(ctx, subExprs.states.values, proxy = proxy),
      subExprs.exprCodesNeedEvaluate, subExprs.states)

  }

  private def topLevelProxy[T](ctx: CodegenContext, e: T, ev: ExprCode, useFreshName: Boolean = true,
                               rewriteCode: (ExprCode, GlobalValue, () => String) => ExprCode = (a,_,_) => a,
                               valueName: String = "value",
                               isNullName: String = "isNull")(implicit nt: NullableType[T]) = {
    val value = nt.global(ctx.addMutableState(nt.javaType(e), valueName, useFreshName = useFreshName), e)
    lazy val nullT = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, isNullName, useFreshName = useFreshName)
    val uEv = rewriteCode(ev, value, () => nullT)

    val (code, isNull) =
      if (nt.isNullable(e)) {
        val isNull = nullT
        (
          s"""
             |${uEv.code}
             |$isNull = ${uEv.isNull};
             |$value = ${uEv.value};
              """.stripMargin, JavaCode.isNullGlobal(isNull))
      } else {
        (
          s"""
             |${uEv.code}
             |$value = ${uEv.value};
              """.stripMargin, FalseLiteral)
      }
    val exprCode = ExprCode(isNull, value)
    (code, exprCode)
  }

  private def createVar(ctx: CodegenContext, f: DataType, isNullable: Boolean, rowName: String, i: Int): ExprCode = {
    val value = JavaCode.global(
      ctx.addMutableState(CodeGenerator.javaType(f), "value"),
      f)
    val typ = JavaCode.javaType(f)

    val (code, isNull) =
      if (isNullable) {
        val isNull = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "isNull")
        (
          code"""
             $value = ${CodeGenerator.getValue(rowName, f, i.toString)};
             $isNull = $rowName.isNullAt($i);
              """, JavaCode.isNullGlobal(isNull))
      } else {
        (
          code"""
             $value =  ${CodeGenerator.getValue(rowName, f, i.toString)};
              """, FalseLiteral)
      }

    ExprCode(code, isNull, value)
  }

  private def createVars(ctx: CodegenContext, struct: StructType, rowName: String): Seq[ExprCode] =
    struct.zipWithIndex.map { case (f, i) =>
      createVar(ctx, f.dataType, f.nullable, rowName, i)
    }

  // $COVERAGE-OFF$
  // for use in debugging rows generated by create's generated code
  def debug(row: InternalRow, any: Any): Unit = {
    println("here")
  }
  // $COVERAGE-ON$

  // directly taken from wholestage
  protected def evaluateVariables(variables: Seq[ExprCode]): String = {
    val evaluate = variables.filter(_.code.nonEmpty).map(_.code.toString).mkString("\n")
    variables.foreach(_.code = EmptyBlock)
    evaluate
  }

  def create[I: Encoder, O: Encoder](
                      expressions: Seq[Expression], toSize: Int): DecoderOpEncoderProjection[I,O] = {

    val iEnc = implicitly[Encoder[I]]
    val oEnc = implicitly[Encoder[O]]
    val exprFrom = ShimUtils.expressionEncoder(iEnc).resolveAndBind().serializer
    val exprFromType = ShimUtils.expressionEncoder(iEnc).resolveAndBind().deserializer.dataType
    val exprTo = ShimUtils.expressionEncoder(implicitly[Encoder[O]]).resolveAndBind().deserializer

    if (expressions.exists(_.exists{
      case s: PlanExpression[_] => true
      case _ => false
      })) {
      throw new QualityException(NO_QUERY_PLANS)
    }

    val (resTypeIsStruct, resType) =
      if (toSize == 1)
        (expressions.last.dataType.isInstanceOf[StructType],
          expressions.last.dataType.asInstanceOf[StructType])
      else
        (false, null)

    val ctx = newCodeGenContext()

    // the code references to the other fields is already present inside of expressions, works for input_row based,
    // but not wholestage approach, this is performed by all the CodegenSupport execs via attribute lookups
    val exprsToUse = expressions.drop( expressions.length - toSize)

    ctx.INPUT_ROW = "enc"
    val input = ctx.freshName("_i_value")
    val nullInput = ctx.freshName("_i_isNull")
    val cast = JavaCode.boxedType(exprFromType)

    val topVarName = ctx.addMutableState(JavaCode.javaType(exprFromType).code, input, useFreshName = false)
    val topNullName = ctx.addMutableState("boolean", nullInput, useFreshName = false)
    val topVar: ExprCode = ExprCode(code =
      code"""
            $topVarName = ($cast) _i;
            $topNullName = $topVarName == null;
            """,
      JavaCode.variable(topNullName, classOf[Boolean]), JavaCode.variable(topVarName, iEnc.clsTag.runtimeClass))

    val topVarRef = Seq(topVar.copy(code = EmptyBlock))

    ctx.currentVars = topVarRef
    val (encSubExprsCode, encSubExprInputs, encSubExprStates) =
      subExprElim(ctx, exprFrom, proxy = false)

    val encProjectionCodes = projections(ctx, exprFrom, "encRow").toIndexedSeq // streams suck

    ctx.currentVars = null
    val encProjections = ctx.splitExpressionsWithCurrentInputs(encProjectionCodes.map(_._2))
    val encUpdates = ctx.splitExpressionsWithCurrentInputs(encProjectionCodes.map(_._3))

    val exprVals = encProjectionCodes.map(_._1)

    ctx.INPUT_ROW = "i"

    ctx.currentVars = exprVals ++ topVarRef

    // Evaluate all the subexpressions.
    // only for input_row projections val evalSubexpr = ctx.subexprFunctionsCode
    val (projectionSubExprsCode, projectionSubExprInputs, projectionSubExprStates) =
      subExprElim(ctx, exprsToUse, proxy = true)

    // we need InputRow to start with, after that we can bind to name
    ctx.currentVars = exprVals ++ projectionSubExprStates.map(_._2.eval).map(_.copy(code = EmptyBlock)) ++ topVarRef

    // generate the full set
    val projectionCodes = projections(ctx, expressions, "mutableRow", projectionSubExprStates).toIndexedSeq // streams suck

    ctx.currentVars = null
    val allProjections = ctx.splitExpressionsWithCurrentInputs(projectionCodes.map(_._2))
    val allUpdates = ctx.splitExpressionsWithCurrentInputs(projectionCodes.map(_._3))

    // create vars for the interesting results
    val oexprVals = projectionCodes.drop( expressions.length - toSize).map(_._1.copy(code = EmptyBlock))

    val decSetupCodes =
      if (toSize == 1 && resTypeIsStruct)
        createVars(ctx, resType, oexprVals.head.value)
      else
        oexprVals

    val decSetupCode = ctx.splitExpressionsWithCurrentInputs(decSetupCodes.map(_.code.code))

    ctx.currentVars = decSetupCodes.map(_.copy(code = EmptyBlock))

    ///ctx.currentVars = null // it requires the vars to be provided for each input param
    ctx.INPUT_ROW = "dec"
    val (decSubExprsCode, decSubExprInputs, decSubExprStates) =
      subExprElim(ctx, Seq(exprTo), proxy = true)

    val decProjectionCodes = projections(ctx, Seq(exprTo), "decRow", decSubExprStates).toIndexedSeq // streams suck

    ctx.currentVars = null
    val decProjections = ctx.splitExpressionsWithCurrentInputs(decProjectionCodes.map(_._2))
    val decUpdates = ctx.splitExpressionsWithCurrentInputs(decProjectionCodes.map(_._3))

    val result = decProjectionCodes.last._1.value

    val codeBody = s"""
      public java.lang.Object generate(Object[] references) {
        return new SpecificMutableProjection(references);
      }

      class SpecificMutableProjection extends ${classOf[DecoderOpEncoderProjection[I,O]].getName}<Object, Object> {

        private Object[] references;
        private InternalRow inRow;
        private InternalRow mutableRow;
        private InternalRow encRow;
        private InternalRow decRow;
        private InternalRow interim;
        ${ctx.declareMutableStates()}

        public SpecificMutableProjection(Object[] references) {
          this.references = references;
          mutableRow = new $genericMutableRowType(${expressions.size});
          inRow = new $genericMutableRowType(1);
          encRow = new $genericMutableRowType(${exprFrom.length});
          decRow = new $genericMutableRowType(${toSize});
          ${
            if (toSize == 1 && resTypeIsStruct)
              ""
            else
              s"interim = new $genericMutableRowType(${toSize});"
          }
          ${ctx.initMutableStates()}
        }

        public ${classOf[DecoderOpEncoderProjection[I,O]].getName} newInstance() {
          return new SpecificMutableProjection(references);
        }

        public void initialize(int partitionIndex) {
          ${ctx.initPartition()}
        }

        //public ${implicitly[Encoder[O]].clsTag.runtimeClass.getName} apply(${implicitly[Encoder[I]].clsTag.runtimeClass.getName} _i) {
        public java.lang.Object apply(java.lang.Object _i) {
          ${ctx.initMutableStates()}
          inRow.update(0, _i);
          ${topVar.code}
          // input processing sub exprs need the enc
          InternalRow enc = (InternalRow) inRow;
          // the iterator isn't part of the wholestage so it gets the ctx level subexprs
          // setup
          ${evaluateVariables(encSubExprInputs)}
          // enc subexprs
          $encSubExprsCode

          // encoding from object done by subexprs
          $encProjections
          $encUpdates

          // may be in subexprs
          InternalRow i = (InternalRow) encRow;

          // common sub-expressions
          // setup
          ${evaluateVariables(projectionSubExprInputs)}
          // the code
          $projectionSubExprsCode

          // projections doing the actual work
          $allProjections
          // copy all the results into MutableRow
          $allUpdates

          // uncomment to debug the output, extraProjection can introduce extra fields..
          // com.sparkutils.quality.impl.GenerateDecoderOpEncoderVarProjection.debug(mutableRow, $topVarName);

          // prepare input vars for the decoding
          $decSetupCode

          // prepare input row for decoding to end user type
          ${
            if (toSize == 1 && resTypeIsStruct)
              s"interim = mutableRow.getStruct(${expressions.length - 1}, ${resType.length});"
            else
              (for(i <- 0 until toSize) yield
                s"interim.update($i, ${CodeGenerator.getValue("mutableRow", expressions((expressions.length - toSize) + i).dataType, ((expressions.length - toSize) + i).toString)});"
                ).mkString("\n")
          }

          decode();

          return /* (${implicitly[Encoder[O]].clsTag.runtimeClass.getName}) */ $result; // decRow.get(0, new org.apache.spark.sql.types.ObjectType(Object.class));
        }

        void decode() {
          InternalRow dec = (InternalRow) interim;
          // dec subexprs setup
          ${evaluateVariables(decSubExprInputs)}
          // the code
          $decSubExprsCode
          // decoding projections
          $decProjections
          // decoding updates, should only be for index 0
          //decUpdates
        }

        // emitExtraCode
        ${ctx.emitExtraCode()}

        // added functions
        ${ctx.declareAddedFunctions()}
      }
    """

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    logDebug(s"code for ${expressions.mkString(",")}:\n${CodeFormatter.format(code)}")

    val (clazz, _) = CodeGenerator.compile(code)
    val initial = clazz.generate(ctx.references.toArray).asInstanceOf[DecoderOpEncoderProjection[I,O]]
    if (isCopyNeeded(expressions))
      new DecoderOpEncoderProjection[I,O] {

        override def apply(input: I): O = initial(input)

        // needs a fresh tree copy for each newInstance, so we need to proxy it
        override def newInstance: DecoderOpEncoderProjection[I, O] =
          create[I, O] (
            expressions.map(e => e.transformUp { case t => t.withNewChildren(t.children) }), toSize)

        override def initialize(partitionIndex: Int): Unit = initial.initialize(partitionIndex)
      }
    else
      // with no stateful CodegenFallback's the generated newInstance function is correct
      initial
  }

}
