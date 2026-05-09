package org.apache.spark.sql.catalyst.expressions.codegen

import java.lang.reflect.{Field, Method}
import scala.collection.mutable

object QualityCodeGenUtils {

  lazy val currentIndex: Field = {
    val clazz = classOf[CodegenContext#MutableStateArrays]
    val currentIndex = clazz.getDeclaredFields.filter(_.getName.contains("currentIndex")).head
    currentIndex.setAccessible(true)
    currentIndex
  }

  lazy val stateNames: Method = {
    val clazz = classOf[CodegenContext]
    val stateNamesF = clazz.getDeclaredMethods.filter(_.getName.contains("mutableStateNames")).head
    stateNamesF.setAccessible(true)
    stateNamesF
  }

  /**
   * clone is needed as the inner MutableStateArrays class binds to the original class
   * @param in
   * @param target
   * @return
   */
  def clone(in: CodegenContext#MutableStateArrays, target: CodegenContext): target.MutableStateArrays = {
    val out = new target.MutableStateArrays()
    out.arrayNames.clear()
    out.arrayNames.addAll(in.arrayNames)

    currentIndex.set(out, currentIndex.get(in))

    out
  }

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
    thisCtx.currentVars = ctx.currentVars
    thisCtx.references.addAll(ctx.references)

    thisCtx.partitionInitializationStatements.addAll(ctx.partitionInitializationStatements)
    thisCtx.arrayCompactedMutableStates.addAll(ctx.arrayCompactedMutableStates.
      map(p => p._1 -> clone(p._2, thisCtx)))
    thisCtx.inlinedMutableStates.addAll(ctx.inlinedMutableStates)
    thisCtx.mutableStateInitCode.addAll(ctx.mutableStateInitCode)

    val thisStateNames = stateNames.invoke(thisCtx).asInstanceOf[mutable.HashSet[String]]
    val ctxStateNames = stateNames.invoke(ctx).asInstanceOf[mutable.HashSet[String]]

    thisStateNames.addAll(ctxStateNames)
    ///thisCtx.subExprEliminationExprs

    thisCtx
  }

  def bump(outerctx: CodegenContext, ctx: CodegenContext): Unit = {
    // now copy over the other references
    for( i <- outerctx.references.size until ctx.references.size) {
      outerctx.references.addOne(ctx.references(i))
    }
  }
}
