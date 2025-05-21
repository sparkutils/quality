package com.sparkutils.quality.impl

import com.sparkutils.quality.QualityException
import com.sparkutils.quality.sparkless.impl.DecoderOpEncoderProjection
import com.sparkutils.quality.sparkless.impl.Processors.{NO_QUERY_PLANS, isCopyNeeded}
import org.apache.spark.sql.{Encoder, ShimUtils}
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.NoOp
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types.StructType

/**
 * CODE is based on MutableProjection and generates a transformation between two encoders over a middle operation.
 * The generated class itself can create new instances directly, unlike Spark projections that need to go through the
 * codegen source generation cycle, only the compilation is cached.
 *
 * If the expression tree contains stateful expressions with codegenfallback the code must be regenerated against a
 * fresh tree.
 */
object GenerateDecoderOpEncoderProjection extends CodeGenerator[Seq[Expression], DecoderOpEncoderProjection[_,_]] {

  protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer.execute)

  protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    bindReferences(in, inputSchema)

  def generate[I: Encoder, O: Encoder](expressions: Seq[Expression],
                                       useSubexprElimination: Boolean): DecoderOpEncoderProjection[I,O] = {
    create(canonicalize(expressions), useSubexprElimination)
  }

  protected def create(expressions: Seq[Expression]): DecoderOpEncoderProjection[_,_] = ???

  def projections(ctx: CodegenContext, expressions: Seq[Expression], mutableRow: String, useSubexprElimination: Boolean = false) = {
    val validExpr = expressions.zipWithIndex.filter {
      case (NoOp, _) => false
      case _ => true
    }
    val exprVals = ctx.generateExpressions(validExpr.map(_._1), useSubexprElimination)

    // 4-tuples: (code for projection, isNull variable name, value variable name, column index)
    val projectionCodes: Seq[(String, String)] = validExpr.zip(exprVals).map {
      case ((e, i), ev) =>
        val value = JavaCode.global(
          ctx.addMutableState(CodeGenerator.javaType(e.dataType), "value"),
          e.dataType)
        val (code, isNull) = if (e.nullable) {
          val isNull = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "isNull")
          (s"""
              |${ev.code}
              |$isNull = ${ev.isNull};
              |$value = ${ev.value};
            """.stripMargin, JavaCode.isNullGlobal(isNull))
        } else {
          (s"""
              |${ev.code}
              |$value = ${ev.value};
            """.stripMargin, FalseLiteral)
        }
        val update = CodeGenerator.updateColumn(
          mutableRow,
          e.dataType,
          i,
          ExprCode(isNull, value),
          e.nullable)
        (code, update)
    }
    projectionCodes
  }

  private def create[I: Encoder, O: Encoder](
                      expressions: Seq[Expression],
                      useSubexprElimination: Boolean): DecoderOpEncoderProjection[I,O] = {

    val iEnc = implicitly[Encoder[I]]
    val exprFrom = ShimUtils.expressionEncoder(iEnc).resolveAndBind().serializer
    val exprTo = ShimUtils.expressionEncoder(implicitly[Encoder[O]]).resolveAndBind().deserializer

    val hasSubQuery =
      expressions.map(_.collectFirst {
        case s: PlanExpression[_] => true
      }.getOrElse(false))

    if (hasSubQuery.contains(true)) {
      throw new QualityException(NO_QUERY_PLANS)
    }

    val toSize =
      exprTo.dataType match {
        case s: StructType => s.length
        case _ => 1
      }

    val resTypeIsStruct = expressions(exprFrom.length).dataType.isInstanceOf[StructType]
    val resType =
      if (resTypeIsStruct)
        expressions(exprFrom.length).dataType.asInstanceOf[StructType]
      else
        null
    val offset = exprFrom.length

    val ctx = newCodeGenContext()

    val projectionCodes = projections(ctx, expressions, "mutableRow", useSubexprElimination)

    // Evaluate all the subexpressions.
    val evalSubexpr = ctx.subexprFunctionsCode

    val allProjections = ctx.splitExpressionsWithCurrentInputs(projectionCodes.map(_._1))
    val allUpdates = ctx.splitExpressionsWithCurrentInputs(projectionCodes.map(_._2))

    ctx.INPUT_ROW = "enc"
    val encProjectionCodes = projections(ctx, exprFrom, "encRow")

    val encProjections = ctx.splitExpressionsWithCurrentInputs(encProjectionCodes.map(_._1))
    val encUpdates = ctx.splitExpressionsWithCurrentInputs(encProjectionCodes.map(_._2))

    ctx.INPUT_ROW = "dec"
    val decProjectionCodes = projections(ctx, Seq(exprTo), "decRow")

    val decProjections = ctx.splitExpressionsWithCurrentInputs(decProjectionCodes.map(_._1))
    val decUpdates = ctx.splitExpressionsWithCurrentInputs(decProjectionCodes.map(_._2))

    val codeBody = s"""
      public java.lang.Object generate(Object[] references) {
        return new SpecificMutableProjection(references);
      }

      class SpecificMutableProjection extends ${classOf[DecoderOpEncoderProjection[I,O]].getName}<${iEnc.clsTag.runtimeClass.getName}, ${implicitly[Encoder[O]].clsTag.runtimeClass.getName}> {

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
            if (expressions.length - offset == 1 && resTypeIsStruct)
              ""
            else
              s"interim = new $genericMutableRowType(${expressions.length - offset});"
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
          inRow.update(0, _i);
          InternalRow enc = (InternalRow) inRow;
          $encProjections
          $encUpdates

          InternalRow i = (InternalRow) encRow;
          $evalSubexpr
          $allProjections
          // copy all the results into MutableRow
          $allUpdates

          ${
            if (expressions.length - offset == 1 && resTypeIsStruct)
              s"interim = mutableRow.getStruct(${exprFrom.length}, ${resType.length});"
            else
              (for(i <- 0 until (expressions.length - offset)) yield
               // s"interim.update($i, mutableRow.get(${offset + i}, org.apache.spark.sql.types.${exprsToUse(offset + i).dataType.typeName}));"
                s"interim.update($i, ${CodeGenerator.getValue("mutableRow", expressions(offset + i).dataType, (offset + i).toString)});"
                ).mkString("\n")
          }

          InternalRow dec = (InternalRow) interim;
          $decProjections
          $decUpdates

          return (${implicitly[Encoder[O]].clsTag.runtimeClass.getName}) decRow.get(0, new org.apache.spark.sql.types.ObjectType(Object.class));
        }


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
            expressions.map(e => e.transformUp { case t => t.withNewChildren(t.children) }),
            useSubexprElimination)

        override def initialize(partitionIndex: Int): Unit = initial.initialize(partitionIndex)
      }
    else
      // with no stateful CodegenFallback's the generated newInstance function is correct
      initial
  }
}
