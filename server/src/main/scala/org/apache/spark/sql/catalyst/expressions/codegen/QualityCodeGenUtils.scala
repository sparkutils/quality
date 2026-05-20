package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.sql.ShimUtils
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.ShimExprUtils
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator.{JAVA_BOOLEAN, javaType}
import org.apache.spark.sql.catalyst.expressions.{EquivalentExpressions, Expression}

import scala.collection.mutable.ArrayBuffer

object QualityCodeGenUtils {

  /**
   * In order to generate wholestagecodegen inputs from previous currentvars may be needed.  As such the state needs
   * to be practically carbon copied and there is no process in Spark for that, because reasonably there was no attempt
   * to have 100k+ worth of expressions
   *
   * @param ctx
   * @return
   */
  def clone(ctx: CodegenContext): CodegenContext = {

    val thisCtx = new CodegenContext()
    thisCtx.INPUT_ROW = ctx.INPUT_ROW
    thisCtx.currentVars = if (ctx.currentVars ne null) { ctx.currentVars.map(_.copy()) } else null
    thisCtx.references.++=(ctx.references)

    thisCtx
  }

  // also used by Collector
  def addOne[T](output: ArrayBuffer[T], an: T): Unit = output.+=(an)

  def bump(outerctx: CodegenContext, ctx: CodegenContext): Unit = {
    // now copy over the other references
    for( i <- outerctx.references.size until ctx.references.size) {
      addOne(outerctx.references, ctx.references(i))
    }
  }

  /**
   * Unlike the Spark version it allows any op to take place
   * @param ctx
   * @param newSubExprEliminationExprs
   * @param f
   * @tparam T
   * @return
   *
  def withSubExprEliminationExprs[T](ctx: CodegenContext, newSubExprEliminationExprs: Map[QualityExprUtils.ExprEquals, SubExprEliminationState])(
    f: => T): T = {
    val oldsubExprEliminationExprs = ctx.subExprEliminationExprs
    ctx.subExprEliminationExprs = newSubExprEliminationExprs

    val genCodes = f

    // Restore previous subExprEliminationExprs
    ctx.subExprEliminationExprs = oldsubExprEliminationExprs
    genCodes
  }
*/
  /**
   * Unlike the Spark version it allows any op to take place
   * @param ctx
   * @param newSubExprEliminationExprs
   * @param f
   * @tparam T
   * @return
   */
  def withSubExprEliminationExprs[T](ctx: CodegenContext, newSubExprEliminationExprs: Map[ShimExprUtils.ExprEquals, SubExprEliminationState])(
    f: => T): T = {
    val oldsubExprEliminationExprs = ctx.subExprEliminationExprs
    ctx.subExprEliminationExprs = newSubExprEliminationExprs

    val genCodes = f

    // Restore previous subExprEliminationExprs
    ctx.subExprEliminationExprs = oldsubExprEliminationExprs
    genCodes
  }

  /**
   * Based on the Spark subexpressionElimination code.  Following the approach from Spark any statefuls are
   * copied before codegen.
   * This, alongside forcing initialise to be called, as part of #131 require the test cases to change.
   */
  def nonWholeStageSubexpressionElimination(ctx: CodegenContext, dirtyExpressions: Seq[Expression]): String = {
    val expressions = ShimUtils.copyStateful(dirtyExpressions)
    import ctx._
    val equivalentExpressions: EquivalentExpressions = new EquivalentExpressions

    // Add each expression tree and compute the common subexpressions.
    expressions.foreach(equivalentExpressions.addExprTree(_))
    var subexprFunctions = ""
    // Get all the expressions that appear at least twice and set up the state for subexpression
    // elimination.
    val commonExprs = ShimExprUtils.getAllEquivalentExprs(equivalentExpressions)
    commonExprs.foreach { expr =>
      val fnName = freshName("subExpr")
      val isNull = addMutableState(JAVA_BOOLEAN, "subExprIsNull")
      val value = addMutableState(javaType(expr.dataType), "subExprValue")

      // Generate the code for this expression tree and wrap it in a function.
      val eval = expr.genCode(ctx)
      val fn =
        s"""
           |private void $fnName(InternalRow $INPUT_ROW) {
           |  ${eval.code}
           |  $isNull = ${eval.isNull};
           |  $value = ${eval.value};
           |}
           """.stripMargin

      // Add a state and a mapping of the common subexpressions that are associate with this
      // state. Adding this expression to subExprEliminationExprMap means it will call `fn`
      // when it is code generated. This decision should be a cost based one.
      //
      // The cost of doing subexpression elimination is:
      //   1. Extra function call, although this is probably *good* as the JIT can decide to
      //      inline or not.
      // The benefit doing subexpression elimination is:
      //   1. Running the expression logic. Even for a simple expression, it is likely more than 3
      //      above.
      //   2. Less code.
      // Currently, we will do this for all non-leaf only expression trees (i.e. expr trees with
      // at least two nodes) as the cost of doing it is expected to be low.

      val subExprCode = s"${addNewFunction(fnName, fn)}($INPUT_ROW);"
      subexprFunctions += subExprCode
      val state = ShimExprUtils.state(
        ExprCode(code"$subExprCode",
          JavaCode.isNullGlobal(isNull),
          JavaCode.global(value, expr.dataType)))
      ShimExprUtils.addSubExpr(ctx, expr, state)
    }

    subexprFunctions
  }


}
