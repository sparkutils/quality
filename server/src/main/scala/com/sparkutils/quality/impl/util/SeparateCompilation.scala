package com.sparkutils.quality.impl.util

import com.sparkutils.quality.VersionedId
import com.sparkutils.quality.impl.RuleEngineRunnerUtils.CompilerTerms
import org.apache.spark.sql.ClassicQualitySparkUtils.genParams
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, CodegenContext, ExprCode, QualityCodeGenUtils, QualityExprUtils}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._

/**
 * Implemented by the separate compilation to allow for nondeterministic / stateful
 */
trait InitPartitionSimple {

  /**
   * Initializes internal states given the current partition index.
   * This is used by nondeterministic expressions to set initial states.
   * The default implementation does nothing.
   */
  def initialize(partitionIndex: Int): Unit

}

/**
 * Implemented by the separate compilation to allow for nondeterministic / stateful
 *
 * Wholestage has a lot more bookkeeping
 *
 */
trait InitPartitionWholeStage {

  /**
   * Initializes internal states given the current partition index.
   * This is used by nondeterministic expressions to set initial states.
   * The default implementation does nothing.
   */
  def initialize(index: Int, inputs: Array[Iterator[_]]): Unit

}

trait ParamsAndName[T] {
  def apply(t: T): (ParameterInformation, String)
}
object ParamsAndName {
  implicit val direct: ParamsAndName[(ParameterInformation, String)] = new ParamsAndName[(ParameterInformation, String)] {

    override def apply(t: (ParameterInformation, String)): (ParameterInformation, String) = t
  }
  implicit val viaTerms: ParamsAndName[CompilerTerms] = new ParamsAndName[CompilerTerms] {

    override def apply(t: CompilerTerms): (ParameterInformation, String) = (t.parameterInformation, t.runnerClassName)
  }
}

object SeparateCompilation {

  /**
   * creates a new clazz, but it is linked and created in the outer context.  Used by ExpressionRunner and RuleRunner
   */
  def runnerCompilation(outerctx: CodegenContext, terms: CompilerTerms, ctx: CodegenContext, codeBody: ExprCode,
                        ev: ExprCode, ruleSuiteId: VersionedId): (CodeAndComment, ExprCode) =
    SeparateCompilation.runnerCompilation(outerctx, terms.parameterInformation,
      terms.runnerClassName, ctx, codeBody, ev, ruleSuiteId, "")

  def runnerCompilation(outerctx: CodegenContext, terms: CompilerTerms, ctx: CodegenContext, codeBody: ExprCode,
                        ev: ExprCode, ruleSuiteId: VersionedId, subExpressions: String): (CodeAndComment, ExprCode) =
    SeparateCompilation.runnerCompilation(outerctx, terms.parameterInformation,
      terms.runnerClassName, ctx, codeBody, ev, ruleSuiteId, subExpressions)

  def withSubExpressions[T: ParamsAndName](
      theThis: Expression, children: Seq[Expression],
      outerCtx: CodegenContext, ev: ExprCode, ruleSuiteId: VersionedId)(
      generate: (CodegenContext,Int) => (T, ExprCode) ): (CodeAndComment, ExprCode) = {

    val ruleRunnerExpressionIdx = outerCtx.references.length
    outerCtx.references += theThis
    val ctx = QualityCodeGenUtils.clone(outerCtx)

    val params = genParams(ctx, theThis)

    val ((compilerTerms, codeBody), subExpressionCode) =
      if (ctx.currentVars eq null) {
        // only fails on "via ProcessFactory with Avro inputs" RowToRowTest shows it doesn't always work for projections

        val subExpressionCode = QualityCodeGenUtils.nonWholeStageSubexpressionElimination(ctx, children)

        (generate(ctx, ruleRunnerExpressionIdx), subExpressionCode)
      } else {
        val subExprs = ctx.subexpressionEliminationForWholeStageCodegen(children)
        val subExpressionCode = QualityExprUtils.evaluateSubExprEliminationState(ctx, subExprs)

        (QualityCodeGenUtils.withSubExprEliminationExprs(ctx, subExprs.states) {
          generate(ctx, ruleRunnerExpressionIdx)
        }, subExpressionCode)
      }

    // need to use the top level params as they are isolated, internally the params will shift to using any subexprs
    runnerCompilation(outerctx = outerCtx, params,
      implicitly[ParamsAndName[T]].apply(compilerTerms)._2, ctx = ctx, codeBody = codeBody, ev = ev,
      ruleSuiteId = ruleSuiteId, subExpressions = subExpressionCode)
  }

  /**
   * creates a new clazz, but it is linked and created in the outer context.  Used by the engines
   */
  def runnerCompilation(outerctx: CodegenContext, parameterInformation: ParameterInformation,
                        runnerClassName: String, ctx: CodegenContext, codeBody: ExprCode,
                        ev: ExprCode, ruleSuiteId: VersionedId, subExpressions: String = ""):
    (CodeAndComment, ExprCode) = {
    val fullParams = parameterInformation

    // TODO - As Spark has already added ctx vars for codebody null and value, we need to remove them
    val (initParamDef, initDecl, initConversion, initType, wholeStage) =
      if ((ctx.INPUT_ROW eq null) && parameterInformation.params.nonEmpty)
        // wholestage
        ("int index, scala.collection.Iterator[] inputs_ppp",
          "private int partitionIndex;\n private scala.collection.Iterator[] inputs;\n",
          """partitionIndex = (Integer) index;
            this.inputs = (scala.collection.Iterator[]) inputs_ppp;""", classOf[InitPartitionWholeStage].getName, true)
      else
        ("int partitionIndex","","", classOf[InitPartitionSimple].getName, false)

    val id = s"${ruleSuiteId.id}_${ruleSuiteId.version}".replaceAll("-","__")

    // TODO maximum is 255 params, the codegenerator code has no upper limit, but it's 22 for function, need a array wrapper approach
    val runnerClassBody = s"""
      public RunnerCompilation$id generate(Object[] references) {
        return new RunnerCompilation$id(references);
      }

      class RunnerCompilation$id extends ${fullParams.aritySafeApplyType("scala.runtime.AbstractFunction")} implements $initType {
        private final Object[] references;
        $initDecl
        // ctx mutable states
        ${ctx.declareMutableStates()}
        // extra params global (outer ctx subexprs and state)
        ${fullParams.aritySafeParamDecl}

        public RunnerCompilation$id(Object[] references) {
          this.references = references;
        }

        public void initialize($initParamDef) {
          ${ctx.initMutableStates()}
          $initConversion

          ${ctx.initPartition()}
        }

        public java.lang.Object apply(${fullParams.aritySafeParamDef}) {
          // here to use extraApplyParamDef
          ${fullParams.aritySafeParamConversion}

          // this context common sub exprs
          $subExpressions

          // rule runner code body
          ${codeBody.code}
          return ${codeBody.isNull} ? ((Object)null) : ((Object)${codeBody.value});
        }

        ${ctx.emitExtraCode()}

        ${ctx.declareAddedFunctions()}
      }
    """

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(runnerClassBody, ctx.getPlaceHolderToComments()))

    val ruleRunnerExpressionIdx = outerctx.references.size - 1
    // the variable
    val funX = fullParams.aritySafeApplyType("scala.Function")

    // update the state to the current ctx
    QualityCodeGenUtils.bump(outerctx, ctx)
    // this needs to be after bump so the states aren't reset
    val runner = outerctx.addMutableState(funX, "runner", initFunc = // new reference stack
      v => s"$v = ($funX) (($runnerClassName) references[$ruleRunnerExpressionIdx]).generatorClazz().generate( references );")

    val res = ev.copy( code =
      code"""
        // push to top
        ${parameterInformation.pushToTop}
        // Call to RuleSuite Id(${ruleSuiteId.id},${ruleSuiteId.version})
        InternalRow ${ev.value} = (InternalRow) (($funX)$runner).apply(${fullParams.aritySafeParamCall});
        boolean ${ev.isNull} = false;
          """)

    outerctx.addPartitionInitializationStatement(
      if (wholeStage)
        s"""
          (($initType )$runner).initialize(partitionIndex, inputs);
        """
      else
        s"""
          (($initType )$runner).initialize(partitionIndex);
        """
    )

    (code, res)
  }
}
