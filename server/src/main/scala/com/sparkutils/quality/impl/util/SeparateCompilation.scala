package com.sparkutils.quality.impl.util

import com.sparkutils.quality.VersionedId
import com.sparkutils.quality.impl.RuleEngineRunnerUtils.CompilerTerms
import com.sparkutils.quality.impl.util.ExtraConfig.ConfigMapOps
import com.sparkutils.quality.impl.Runner
import com.sparkutils.shim.codegen.SubExprCodeGen
import org.apache.spark.sql.ClassicQualitySparkUtils.genParams
import org.apache.spark.sql.catalyst.expressions.{Expression, Unevaluable}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, CodeGenerator, CodegenContext, ExprCode, ExprValue, QualityCodeGenUtils, ShimExprUtils}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.types.DataType

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

trait ClazzGenerator[T] {
  def apply(t: T): Int => String
  def outerResultProcessing(t: T): (CodegenContext, ExprValue) => String
  def id(t: T): Int
}

case class SeparateClassGenerator(className: String, extraParams: Seq[ExprValue], index: Int)

object ClazzGenerator {

  def classGen(name: String, ruleRunnerExpressionIdx: Int, index: Int = 0 ) =
    s"(($name) references[$ruleRunnerExpressionIdx]).generatorClazz($index).generate( references )"

  implicit val direct: ClazzGenerator[(ParameterInformation, String)] = new ClazzGenerator[(ParameterInformation, String)] {

    override def apply(t: (ParameterInformation, String)): Int => String = classGen(t._2, _)

    override def outerResultProcessing(t: (ParameterInformation, String)): (CodegenContext, ExprValue) => String = (_,_) => ""

    override def id(t: (ParameterInformation, String)): Int = 0
  }
  implicit val viaTerms: ClazzGenerator[CompilerTerms] = new ClazzGenerator[CompilerTerms] {

    override def apply(t: CompilerTerms): Int => String = classGen(t.runnerClassName, _ )

    override def outerResultProcessing(t: CompilerTerms): (CodegenContext, ExprValue) => String = (_,_) => ""

    override def id(t: CompilerTerms): Int = 0
  }
  implicit val viaName: ClazzGenerator[SeparateClassGenerator] = new ClazzGenerator[SeparateClassGenerator] {

    override def apply(t: SeparateClassGenerator): Int => String = i => classGen(t.className, i, t.index)

    override def outerResultProcessing(t: SeparateClassGenerator): (CodegenContext, ExprValue) => String = {
      case (ctx, e) =>
        val tmpArr = ctx.freshName("tempArr")
        s"""
           Object[] $tmpArr = ((org.apache.spark.sql.catalyst.expressions.GenericInternalRow)${e.code}).values();
           ${t.extraParams.filterNot(_.javaType.isArray).zipWithIndex.map{
              case (v,index) =>
                val cast =
                  if (v.javaType.isPrimitive)
                    CodeGenerator.boxedType(v.javaType.getSimpleName)
                  else
                    v.javaType.getName

                s"${v.code} = ($cast) $tmpArr[$index];"}.mkString("\n")
            }
           """
    }

    override def id(t: SeparateClassGenerator): Int = t.index
  }
}

trait IdGen[I] {
  def gen(i: I): String
  def forComment(i: I): String
}

case class SubCompilation(id: String, forComment: String)

object IdGen {

  implicit def versionedIdGen[T <: VersionedId]: IdGen[T] = new IdGen[T] {

    override def gen(ruleSuiteId: T): String = s"${ruleSuiteId.id}_${ruleSuiteId.version}".replaceAll("-","__")

    override def forComment(i: T): String = s"RuleSuite Id(${i.id},${i.version})"
  }

  implicit val subCompilation: IdGen[SubCompilation] = new IdGen[SubCompilation] {

    override def gen(i: SubCompilation): String = i.id

    override def forComment(i: SubCompilation): String = i.forComment
  }

}

case class GenerateResult[T](resultType: T, resultExpr: ExprCode, extraClasses: Seq[(Int, CodeAndComment)],
                             ignoreTopLevelSubExpressions: Boolean, id: Int = 0)

object SeparateCompilation {

  case class Holder(children: Seq[Expression]) extends Expression with Unevaluable {

    override def nullable: Boolean = ???

    override def dataType: DataType = ???

    protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(newChildren)
  }

  def withSubExpressions[T: ClazzGenerator, I: IdGen](
      theThis: Runner, children: Seq[Expression],
      outerCtx: CodegenContext, ev: ExprCode, id: I,
      createGenerateFunction: Boolean = true, extraParams: Seq[ExprValue] = Seq.empty, useParams: CodegenContext => ParameterInformation = null )(
      generate: (CodegenContext, Int, ParameterInformation) => GenerateResult[T]
    ): (Seq[(Int, CodeAndComment)], ExprCode) = {

    val ruleRunnerExpressionIdx = outerCtx.references.length
    outerCtx.references += theThis
    val ctx = QualityCodeGenUtils.clone(outerCtx)

    val params =
      if (useParams ne null)
        useParams(ctx)
      else
        genParams(ctx, theThis, extraParams)

    val (genResult, subExpressionCode) =
      if (ctx.currentVars eq null) {
        // only fails on "via ProcessFactory with Avro inputs" RowToRowTest shows it doesn't always work for projections

        val subExpressionCode = QualityCodeGenUtils.nonWholeStageSubexpressionElimination(ctx, children)
        val childParams = genParams(ctx, Holder(children), Seq.empty)
        (generate(ctx, ruleRunnerExpressionIdx, childParams), subExpressionCode)
      } else {
        val subExprs = SubExprCodeGen.subexpressionEliminationForWholeStageCodegen(ctx, children)
        val subExpressionCode = ShimExprUtils.evaluateSubExprEliminationState(ctx, subExprs)

        (QualityCodeGenUtils.withSubExprEliminationExprs(ctx, subExprs.states) {
          val childParams = genParams(ctx, Holder(children), Seq.empty)
          generate(ctx, ruleRunnerExpressionIdx, childParams)
        }, subExpressionCode)
      }

    // need to use the top level params as they are isolated, internally the params will shift to using any subexprs
    runnerCompilation(outerctx = outerCtx, params, genResult, ctx = ctx, ev = ev,
      idParam = id, subExpressions = subExpressionCode,
        generateStatsEvery = theThis.extraConfig.int("statsEvery", 0),
      createGenerateFunction
    )
  }

  def className(id: String) = s"RunnerCompilation$id"

  /**
   * creates a new clazz, but it is linked and created in the outer context.  Used by all runners.
   */
  def runnerCompilation[T: ClazzGenerator, I: IdGen](
                                  outerctx: CodegenContext,
                                  parameterInformation: ParameterInformation,
                                  genResult: GenerateResult[T], ctx: CodegenContext,
                                  ev: ExprCode, idParam: I, subExpressions: String = "",
                                  generateStatsEvery: Int = 0,
                                  createGenerateFunction: Boolean = true):
    (Seq[(Int, CodeAndComment)], ExprCode) = {
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

    val id = implicitly[IdGen[I]].gen(idParam)

    val (statsState, statsRowStart, statBeforeCodeBody, statDump) =
      if (generateStatsEvery == 0)
        ("","","","")
      else {
        val rowCount = ctx.freshName("rowCount")
        val accTime = ctx.freshName("accTime")
        val bodyAccTime = ctx.freshName("bodyAccTime")
        val start = ctx.freshName("start")
        val beforeCodeBody = ctx.freshName("statBeforeCodeBody")
        val end = ctx.freshName("end")
        (s"""
          private long $rowCount = 0;
          private long $accTime = 0;
          private long $bodyAccTime = 0;
          """,
          s"""
          $rowCount = $rowCount + 1;
          long $start = System.nanoTime();
          """,
          s"""
          long $beforeCodeBody = System.nanoTime();
          """,
          s"""
          long $end = System.nanoTime();
          $accTime = $accTime + ($end - $start);
          $bodyAccTime = $bodyAccTime + ($end - $beforeCodeBody);
          if ($rowCount == $generateStatsEvery) {
            System.out.println(this.getClass().getName() + " - RunnerCompilation$id avg \t"+ $accTime +"\t"+$bodyAccTime+"\t ns per every \t$generateStatsEvery\t rows");
            System.out.flush();
            $rowCount = 0;
            $accTime = 0;
            $bodyAccTime = 0;
          }
          """)
      }

    val clazzName = className(id)

    val generate =
      if (createGenerateFunction)
        s"""
        public $clazzName generate(Object[] references) {
          return new $clazzName(references);
        }
        """
      else ""

    val splitSubs =
      if (genResult.ignoreTopLevelSubExpressions) // already provided by the grouper
        ""
      else
        splitGlobalSubExprs(ctx, subExpressions)

    val initCode = splitGlobalSubExprs(ctx, ctx.initPartition(), name = "initCode", groupSize = 150)

    // extra params global (outer ctx subexprs and state), must be after code gen and requires QualityCodeGenUtils.clone
    // to reflect/copy freshNames
    fullParams.addAritySafeParamDecl(ctx)

    // TODO maximum is 255 params, the codegenerator code has no upper limit, but it's 22 for function, need a array wrapper approach
    val runnerClassBody = s"""
      $generate

      // main runner
      class $clazzName extends ${fullParams.aritySafeApplyType("scala.runtime.AbstractFunction")} implements $initType {
        private final Object[] references;
        $initDecl
        // ctx mutable states
        ${ctx.declareMutableStates()}
        // stats state
        $statsState

        public $clazzName(Object[] references) {
          this.references = references;
        }

        public void initialize($initParamDef) {
          ${ctx.initMutableStates()}
          $initConversion

          ${initCode}
        }

        public java.lang.Object apply(${fullParams.aritySafeParamDef}) {

          $statsRowStart

          // here to use extraApplyParamDef
          ${fullParams.aritySafeParamConversion}

          // this context common sub exprs
          $splitSubs

          $statBeforeCodeBody

          // rule runner code body
          ${genResult.resultExpr.code}

          // stat dump
          $statDump

          return ${genResult.resultExpr.isNull} ? ((Object)null) : ((Object)${genResult.resultExpr.value});
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
    /*
    declareMutableStates does this:
          // initializer had a one-dimensional array variable
          val baseType = javaType.substring(0, javaType.length - 2)

          s"private $javaType[] $arrayName = new $baseType[$length][];"

    which leaves Runner>[] looking like Runne[] , which is nice, so an extra two spaces are added to the type
     */

    // this needs to be after bump so the states aren't reset
    val runner = outerctx.addMutableState(s"$funX  ", "runner", initFunc = // new reference stack
      v => s"$v = ($funX) ${implicitly[ClazzGenerator[T]].apply(genResult.resultType)(ruleRunnerExpressionIdx)};")

    val res = ev.copy( code =
      code"""
        // push to top
        ${parameterInformation.pushToTop}
        // Call to ${implicitly[IdGen[I]].forComment(idParam)}
        ${fullParams.aritySafeParamCallPrep(outerctx)}
        InternalRow ${ev.value} = (InternalRow) (($funX)$runner).apply(${fullParams.aritySafeParamCall});
        ${implicitly[ClazzGenerator[T]].outerResultProcessing(genResult.resultType)(outerctx, ev.value)}
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

    (Seq(( implicitly[ClazzGenerator[T]].id(genResult.resultType), code)) ++
      genResult.extraClasses , res)
  }

  /**
   * given we are already split for execution via params no args are needed for subexprs, groups in blocks of 250 to keep
   * the main apply functions JITable, then does a split call on them
   */
  def splitGlobalSubExprs(ctx: CodegenContext, subExpressions: String, name: String = "subExprGroup", groupSize: Int = 250): String = {
    val preGrouped = subExpressions.split(";").filter(_.nonEmpty).map(su => s"$su;")
    QualityCodeGenUtils.splitExpressions(ctx, preGrouped.toSeq, groupSize, name, Seq.empty) // 250 chosen to leave headroom, 500 doesn't hit JIT either currently
  }

}
