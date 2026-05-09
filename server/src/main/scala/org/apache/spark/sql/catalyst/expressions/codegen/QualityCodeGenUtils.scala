package org.apache.spark.sql.catalyst.expressions.codegen

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
}
