package com.sparkutils.quality.impl.util

import com.sparkutils.quality.VersionedId
import com.sparkutils.quality.impl.RuleEngineRunnerUtils.CompilerTerms
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, CodegenContext, ExprCode, QualityCodeGenUtils}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
/**
 * Implemented by the separate compilation to allow for nondeterministic / stateful
 */
trait InitPartition {

  /**
   * Initializes internal states given the current partition index.
   * This is used by nondeterministic expressions to set initial states.
   * The default implementation does nothing.
   */
  def initialize(partitionIndex: Int): Unit = {}

}

object SeparateCompilation {

  /**
   * creates a new clazz, but it is linked and created in the outer context.  Used by ExpressionRunner and RuleRunner
   */
  def runnerCompilation(outerctx: CodegenContext, terms: CompilerTerms, ctx: CodegenContext, codeBody: ExprCode,
                        ev: ExprCode, ruleSuiteId: VersionedId): (CodeAndComment, ExprCode) =
    SeparateCompilation.runnerCompilation(outerctx, terms.parameterInformation,
      terms.runnerClassName, ctx, codeBody, ev, ruleSuiteId)

  /**
   * creates a new clazz, but it is linked and created in the outer context.  Used by the engines
   */
  def runnerCompilation(outerctx: CodegenContext, parameterInformation: ParameterInformation,
                        runnerClassName: String, ctx: CodegenContext, codeBody: ExprCode,
                        ev: ExprCode, ruleSuiteId: VersionedId):
    (CodeAndComment, ExprCode) = {
    // TODO - As Spark has already added ctx vars for codebody null and value, we need to remove them
    val (fullParams, extraApplyParamDef, extraApplyParamCall, extraDecl, extraConversion) =
      if ((ctx.INPUT_ROW eq null) && parameterInformation.params.nonEmpty)
        // wholestage
        (parameterInformation.copy(arity = parameterInformation.arity + 2),
          "Object index, Object inputs_ppp, ", "partitionIndex, this.inputs, ",
          "private int partitionIndex;\n private scala.collection.Iterator[] inputs;\n",
          """partitionIndex = (Integer) index;
            this.inputs = (scala.collection.Iterator[]) inputs_ppp;""")
      else
        (parameterInformation, "","","","")

    val id = s"${ruleSuiteId.id}_${ruleSuiteId.version}".replaceAll("-","__")

    // TODO maximum is 255 params, the codegenerator code has no upper limit, but it's 22 for function, need a array wrapper approach
    val runnerClassBody = s"""
      public RunnerCompilation$id generate(Object[] references) {
        return new RunnerCompilation$id(references);
      }

      class RunnerCompilation$id extends ${fullParams.aritySafeApplyType("scala.runtime.AbstractFunction")} implements ${classOf[InitPartition].getName} {
        private final Object[] references;
        $extraDecl
        ${ctx.declareMutableStates()}

        public RunnerCompilation$id(Object[] references) {
          this.references = references;
        }

        public void initialize(int partitionIndex) {
          ${ctx.initPartition()}
        }

        public java.lang.Object apply($extraApplyParamDef ${fullParams.aritySafeParamDef}) {
          $extraConversion
          // here to use extraApplyParamDef
          ${ctx.initMutableStates()}

          ${fullParams.aritySafeParamConversion}

          // this context common
          ${ctx.subexprFunctionsCode}

          ${codeBody.code}
          return ${codeBody.isNull} ? ((Object)null) : ((Object)${codeBody.value});
        }

        ${ctx.emitExtraCode()}

        ${ctx.declareAddedFunctions()}
      }
    """

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(runnerClassBody, ctx.getPlaceHolderToComments()))

    //val (clazz, _) = CodeGenerator.compile(code)

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
        InternalRow ${ev.value} = (InternalRow) (($funX)$runner).apply($extraApplyParamCall ${fullParams.aritySafeParamCall});
        boolean ${ev.isNull} = false;
          """)

    outerctx.addPartitionInitializationStatement(
      s"""
         ((${classOf[InitPartition].getName} )$runner).initialize(partitionIndex);
         """
    )

    (code, res)
  }
}
