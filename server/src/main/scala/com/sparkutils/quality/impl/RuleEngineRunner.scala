package com.sparkutils.quality.impl

import com.sparkutils.quality.impl.RuleRunnerUtils.RuleSuiteResultArray
import com.sparkutils.quality.{Id, impl, _}
import com.sparkutils.quality.QualityException.qualityException
import com.sparkutils.quality.impl.RuleEngineRunnerUtils.{flattenExpressions, outputExpressionType}
import com.sparkutils.quality.impl.RuleRunnerUtils.{genRuleSuiteTerm, packTheId}
import com.sparkutils.quality.impl.imports.RuleEngineRunnerImports
import PackId.packId
import com.sparkutils.quality
import com.sparkutils.quality.impl.DefaultProcessorImpl.DefaultProcessorImplOps
import com.sparkutils.quality.impl.ExpressionRuleExpr.ExpressionRuleOps
import com.sparkutils.quality.impl.GetRealChildren.getRealChildren
import com.sparkutils.quality.impl.RunOnPassProcessorImpl.RunOnPassProcessorImplOps
import com.sparkutils.quality.impl.util.Params.formatParams
import com.sparkutils.quality.impl.util.{NonPassThrough, ParameterInformation, PassThroughCompileEvals, PassThroughEvalOnly}
import org.apache.spark.sql.ClassicQualitySparkUtils.genParams
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.JavaCode.isNullVariable
import org.apache.spark.sql.catalyst.expressions.codegen.{Block, CodeAndComment, CodeFormatter, CodeGenerator, CodegenContext, CodegenFallback, EmptyBlock, ExprCode, GeneratedClass, QualityCodeGenUtils, VariableValue}
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression}
import org.apache.spark.sql.catalyst.util.{GenericArrayData, truncatedString}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ClassicQualitySparkUtils, Column, DataFrame, ShimUtils}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object RuleEngineRunnerImpl {

  /**
   * Creates a column that runs the RuleSuite.  This also forces registering the lambda functions used by that RuleSuite
   * @param ruleSuite The ruleSuite with runOnPassProcessors
   * @param resultDataType The type of the results from runOnPassProcessors - must be the same for all result types
   * @param compileEvals Should the rules be compiled out to interim objects - by default false, allowing optimisations
   * @param debugMode When debugMode is enabled the resultDataType is wrapped in Array of (salience, result) pairs to
   *                  ease debugging
   * @param resolveWith This experimental parameter can take the DataFrame these rules will be added to and pre-resolve
   *                    and optimise the sql expressions, see the documentation for details on when to and not to use this.
   * @param variablesPerFunc Defaulting to 40 allows, in combination with variableFuncGroup allows customisation of
   *                         handling the 64k jvm method size limitation when performing WholeStageCodeGen
   * @param variableFuncGroup Defaulting to 20
   * @param forceRunnerEval Defaulting to false, passing true forces a simplified partially interpreted evaluation
   *                        (compileEvals must be false to get fully interpreted)
   * @param forceTriggerEval Defaulting to false, passing true forces each trigger expression to be compiled
   *                         (compileEvals) and used in place, false instead expands the trigger in-line giving
   *                         possible performance boosts based on JIT
   * @return A Column representing the QualityRules expression built from this ruleSuite
   */
  def ruleEngineRunnerImpl(ruleSuite: RuleSuite, resultDataType: Option[DataType], compileEvals: Boolean = false,
                       debugMode: Boolean = false, resolveWith: Option[DataFrame] = None, variablesPerFunc: Int = 40,
                       variableFuncGroup: Int = 20, forceRunnerEval: Boolean = false, forceTriggerEval: Boolean = false): Column = {
    com.sparkutils.quality.registerLambdaFunctions( ruleSuite.lambdaFunctions )

    val (expressions, indexes, triggerCount) = flattenExpressions(ruleSuite)

    val cleaned = RuleLogicUtils.cleanExprs(ruleSuite)
    val exprs =
      // ExpressionProxy and SubExprEvaluationRuntime cannot be used with compileEvals
      if (compileEvals)
        expressions.map(PassThroughCompileEvals)
      else
        expressions

    // clean out expressions, UnresolvedRelations etc. from subquery usage forceRunnerEval,
    val runner =
      if (forceRunnerEval || resolveWith.isDefined)
        new RuleEngineRunnerEval(cleaned, exprs, resultDataType, compileEvals,
          debugMode, variablesPerFunc, variableFuncGroup, expressionOffsets = indexes,
          forceTriggerEval, triggerCount = triggerCount)
      else
        new RuleEngineRunner(cleaned, exprs, resultDataType, compileEvals,
          debugMode, variablesPerFunc, variableFuncGroup, expressionOffsets = indexes,
          forceTriggerEval, triggerCount = triggerCount)

    ShimUtils.column(
      ClassicQualitySparkUtils.resolveWithOverride(resolveWith).map { df =>
        val resolved = ClassicQualitySparkUtils.resolveExpression(df, runner)

        resolved.withNewChildren(resolved.children.map{
          // replace the expr
          case PassThroughCompileEvals(child) => NonPassThrough(child)
          case child => NonPassThrough(child)
        })
      } getOrElse runner
    )
  }
}

private[quality] object RuleEngineRunnerUtils extends RuleEngineRunnerImports {

  // derive the correct output expression type
  def outputExpressionType(resultDataType: Option[DataType], children: Seq[Expression], triggerCount: Int): DataType =
    resultDataType.getOrElse {
      // CreateArray uses this approach, pretty much what we are looking for
      // as Output Expressions can contain null they must be filtered out or it will default to NullType
      TypeCoercion.findCommonTypeDifferentOnlyInNullFlags(
        children.drop(triggerCount).filterNot(_.dataType == NullType).map(_.dataType)
      ).getOrElse(NullType)
    }

  protected[quality] def flattenExpressions(ruleSuite: RuleSuite, transformOutputExpression: Expression => Expression = identity): (Seq[Expression], Array[Int], Int) = {
    val outputs = mutable.Map.empty[Id, Int]
    var pos = 0
    val outputExpressions = new mutable.ArrayBuffer[Expression](10)
    val indexes = new mutable.ArrayBuffer[Int](300)

    val expressions =
      ruleSuite.ruleSets.flatMap( ruleSet => ruleSet.rules.map(rule => {
        val expr = rule.expression.toImpl.expr

        val idx = outputs.getOrElse(rule.runOnPassProcessor.id, {
            val expr = rule.runOnPassProcessor match {
              case NoOpRunOnPassProcessor.noOp => needsProcessor(ruleSet, rule)
              case r: quality.RunOnPassProcessor => r.toImpl.returnIfPassed.expr
            }
            outputs.put(rule.runOnPassProcessor.id, pos)

            outputExpressions += transformOutputExpression(expr)

            val opos = pos
            pos += 1
            opos
          })

        indexes += idx

        expr
      }))

    if (ruleSuite.defaultProcessor != NoOpDefaultProcessor.noOp) {
      val expr = ruleSuite.defaultProcessor.toImpl.outputExpression.expr
      outputExpressions += transformOutputExpression(expr)
      indexes += pos
    }

    (expressions ++ outputExpressions, indexes.toArray, expressions.size)
  }

  protected def needsProcessor(ruleSet: RuleSet, rule: Rule): Nothing = {
    qualityException(s"You cannot use a RuleEngine, RuleFolder, ExpressionRunner or CollectRunner if any of the rules do not have RunOnPassProcessors set ruleSet ${ruleSet.id}, rule ${rule.id}}")
  }

  // count is not to be trusted, seems some funcs are evaluated twice
  def debugOutput[T](salienceArr: Array[Int], outArrTerm: Array[T], count: Int, default: T): GenericArrayData = {
    val out = new ArrayBuffer[(Int, T)](count + 1)//-1 start so boost by one, may still be too high
    var i = 0
    for( idx <- 0 until salienceArr.length){
      if (outArrTerm(idx) != null) {
        out += (salienceArr(idx) -> outArrTerm(idx))
        i += 1
      }
    }
    if (default != null) {
      out += (DefaultRuleSalience -> default)
    }
    new org.apache.spark.sql.catalyst.util.GenericArrayData(
      out.sortBy(_._1).map( p => InternalRow(p._1, p._2) )
      )
  }

  def flattenSalience(ruleSuite: RuleSuite): Array[Int] =
    ruleSuite.ruleSets.flatMap( ruleSet => ruleSet.rules.map(rule =>
      rule.runOnPassProcessor match {
        case NoOpRunOnPassProcessor.noOp => needsProcessor(ruleSet, rule)
        case r: RunOnPassProcessor => r.salience
      }
    )).toArray

  def flattenEngineIds(ruleSuite: RuleSuite): Array[(Long, Long, Long)] = //Array[(java.lang.Long, java.lang.Long, java.lang.Long)] =
    ruleSuite.ruleSets.flatMap( ruleSet => ruleSet.rules.map(rule =>
      rule.runOnPassProcessor match {
        case NoOpRunOnPassProcessor.noOp => needsProcessor(ruleSet, rule)
        case r: RunOnPassProcessor => (packTheId(ruleSuite.id), packTheId(ruleSet.id), packTheId(rule.id))
      }
    )).toArray

  def reincorporateExpressions(ruleSuite: RuleSuite, expr: Seq[Expression], compileEvals: Boolean, expressionOffsets: Array[Int], triggerCount: Int): RuleSuite =
    reincorporateExpressionsF(ruleSuite, expr, (expr: Expression) => ExpressionWrapper(expr, compileEvals), (e: Expression)=>e, compileEvals, expressionOffsets, triggerCount)

  def reincorporateExpressionsF[T](ruleSuite: RuleSuite, expr: Seq[T], f: T => RuleLogic[_], processorExpression: T => Expression, compileEvals: Boolean, expressionOffsets: Array[Int], triggerCount: Int): RuleSuite = {
    val itr = expr.zipWithIndex.iterator
    ruleSuite.copy(ruleSets = ruleSuite.ruleSets.map(
      ruleSet =>
        ruleSet.copy( rules = ruleSet.rules.map(
          rule => {
            val (nexpr, index) = itr.next()
            val outexpr = expr(triggerCount + expressionOffsets(index))
            rule.copy(expression = f(nexpr), runOnPassProcessor =
              rule.runOnPassProcessor.toImpl.withExpr(OutputExpressionWrapper(processorExpression(outexpr), compileEvals)))
          }
        ))
    ), defaultProcessor =
      if (ruleSuite.defaultProcessor != NoOpDefaultProcessor.noOp)
        ruleSuite.defaultProcessor.toImpl.withExpr(OutputExpressionWrapper(processorExpression(expr.last), compileEvals))
      else
        ruleSuite.defaultProcessor
    )
  }

  def compiledEvalDebug[T](results: InternalRow, output: T): InternalRow =
    InternalRow(results, null, output)

  def compiledEval[T](results: InternalRow, currentSalience: Int, rules: Array[(Long, Long, Long)], currentOutputIndex: Int, output: Array[T]): InternalRow =
    InternalRow(results,
        if (currentSalience == java.lang.Integer.MAX_VALUE)
          null
        else {
          val rule = rules(currentOutputIndex)
          InternalRow(rule._1, rule._2, rule._3)
        },
        if (currentSalience == java.lang.Integer.MAX_VALUE)
          null
        else output(currentOutputIndex)
      )

  case class CompilerTerms(funNames: _root_.scala.collection.Iterator[_root_.scala.Predef.String],
                           utilsName: String, ruleSuitTerm: String, ruleSuiteArrays: String, resArrTerm: String,
                           currentSalience: String, ruleTupleArrTerm: String, currentOutputIndex: String, outArrTerm: String,
                           salienceArrTerm: String, hasAPassTerm: String, currRuleResTerm: String,
                           runnerClassName: String, parameterInformation: ParameterInformation)

  // exprEnd and exprFunEnd take currRuleResTerm as params
  def genCompilerTerms[T: ClassTag](
                       outerctx:  _root_.org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext,
                       ctx:  _root_.org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext,
                       child: Expression, expressionOffsets: Array[Int], realChildren: Seq[Expression],
                       debugMode: Boolean, variablesPerFunc: Int, variableFuncGroup: Int, forceTriggerEval: Boolean,
                       extraResult: (String, Int, String) => String = (_ : String, _: Int, _: String) => "",
                       extraSetup: (String, Int) => String = (_ : String, _: Int) => "",
                       orderOffset: Int => Int = identity,
                       salienceCheck: Boolean = true, sizeAdjustment: Int = 0,
                       exprEnd: String => Block = _ => code"",
                       exprFunEnd: String => Block = _ => code""
                      ):
    CompilerTerms = {
    val i = ctx.INPUT_ROW

    val paramsInfo = genParams(ctx, child)
    import paramsInfo._

    // bind the rules
    val (ruleSuitTerm, termFun) = genRuleSuiteTerm[T](ctx)
    val utilsName = "com.sparkutils.quality.impl.RuleRunnerUtils"

    val hasAPassTerm = ctx.addMutableState("boolean", ctx.freshName("hasAPass"))

    val childrenFuncTerm = termFun("compiledRealChildren", classOf[ExpressionWrapper].getName + "[]")

    val ruleSuiteArrays = ctx.addMutableState(classOf[RuleSuiteResultArray].getName,
      ctx.freshName("ruleSuiteArrays"),
      v => s"$v = $utilsName.ruleSuiteArrays($ruleSuitTerm);"
    )

    val currentSalience = ctx.addMutableState("int", ctx.freshName("currentSalience"),
      v => s"$v = java.lang.Integer.MAX_VALUE;"
    )
    val currentOutputIndex = ctx.addMutableState("int", ctx.freshName("currentOutputIndex"),
      v => s"$v = -1;"
    )

    val offset = expressionOffsets.size + sizeAdjustment

    val ruleRes = "java.lang.Object"
    val resArrTerm = ctx.addMutableState(ruleRes+"[]", ctx.freshName("results"),
      v => s"$v = new $ruleRes[$offset];")

    val currRuleRes = "int"
    val currRuleResTerm = ctx.addMutableState(currRuleRes, ctx.freshName("currRuleRes"),
      v => s"$v = 0;")

    val ruleTupleRes = classOf[Tuple3[_,_,_]].getName
    val ruleTupleArrTerm = ctx.addMutableState(ruleTupleRes+"[]", ctx.freshName("ruleId"),
      v => s"$v = com.sparkutils.quality.impl.RuleEngineRunnerUtils.flattenEngineIds($ruleSuitTerm);")

    val salienceType = "int"
    val salienceArrTerm = ctx.addMutableState(salienceType+"[]", ctx.freshName("salience"),
      v => s"$v = com.sparkutils.quality.impl.RuleEngineRunnerUtils.flattenSalience($ruleSuitTerm);")

    val output = {
      val javaType = realChildren.last.genCode(ctx).value.javaType // last should always be good
      // can't use the primitive type as it can't handle nulls
      if (javaType.isPrimitive) CodeGenerator.boxedType(javaType.getSimpleName) else javaType.getName
    }

    val outArrTerm = ctx.addMutableState(output+"[]", ctx.freshName("output"),
      v => s"$v = new $output[$offset];")

    val triggerRules = realChildren.slice(0, offset)

    def codeGen(exp: Expression, idx: Int, funName: String) = {
      val (evalPre, eval) =
        if (forceTriggerEval)
          ("", s"com.sparkutils.quality.impl.RuleSuiteHelpers.ruleResultToInt($childrenFuncTerm[$idx].eval($i))")
        else {
          val eval = exp.genCode(ctx)

          // auto boxing on databricks doesn't work due to old janino see #82
          val edt = eval.value.javaType
          val theCast = if (edt.isPrimitive) CodeGenerator.boxedType(edt.getSimpleName) else edt.getName

          (eval.code, s"com.sparkutils.quality.impl.RuleLogicUtils.anyToRuleResultInt(${eval.isNull} ? null : ($theCast) ${eval.value})")
        }

      val converted =
        code"""
            $evalPre
            $currRuleResTerm = $eval;

            $resArrTerm[$idx] = (Integer) $currRuleResTerm;
            if ( ( $currRuleResTerm == $PassedInt ) ${if (!debugMode && salienceCheck) s" && ( $currentSalience > $salienceArrTerm[$idx] ) " else "" }) {
              $hasAPassTerm = true;
              $funName($paramsCall${if (paramsCall.isEmpty) "" else ","} $idx);
            } ${if (!debugMode) "" else s"""
              else {
              $outArrTerm[$idx] = null;
            }"""}
            """

      converted
    }

    val index = ctx.freshName(s"triggerIndex")

    val outExprFunTerms =
      for{ i <- 0 until (realChildren.size - offset) } yield {

        val exprFuncName = ctx.freshName(s"outputExprFun$i")

        val exp = realChildren(offset + i)
        val eval = exp.genCode(ctx)

        val body =
          code"""
              ${extraSetup(index, i)} \n
              ${eval.code} \n

              $outArrTerm[$i] = ${eval.isNull} ? null : ($output)${eval.value}; \n
              ${extraResult(s"$outArrTerm[$i]", i, resArrTerm)}
        """

        ctx.addNewFunction(exprFuncName,
          code"""
   private void $exprFuncName($paramsDef${if (paramsDef.isEmpty) "" else ","} int $index) {
            $body

      ${
            if (debugMode)
              s"""
              $currentOutputIndex += 1; \n

              """
            else
              s"""

              $currentSalience = $salienceArrTerm[$index]; \n
              $currentOutputIndex = $index; \n
              """
          }
      }
  """.code
            )


      }

    // ensure ordering and re-use
    val allExpr = triggerRules.zipWithIndex.map { case (_, idx) =>

      val realI = orderOffset(idx)

      val offset = expressionOffsets(realI)
      val funName = outExprFunTerms(offset)
      val trigger = triggerRules(realI) // the original trigger is useless
      val stepWithIf = codeGen(trigger, realI, funName)

      stepWithIf
    }.grouped(variablesPerFunc).grouped(variableFuncGroup)


    CompilerTerms(
      RuleRunnerUtils.generateFunctionGroups(ctx, allExpr, paramsDef, paramsCall, exprEnd = () => exprEnd(currRuleResTerm),
        exprFunEnd = () => exprFunEnd(currRuleResTerm)),
      utilsName, ruleSuitTerm, ruleSuiteArrays, resArrTerm,
      currentSalience, ruleTupleArrTerm, currentOutputIndex, outArrTerm,
      salienceArrTerm, hasAPassTerm, currRuleResTerm,
      runnerClassName = implicitly[ClassTag[T]].runtimeClass.getName, paramsInfo)

  }
  // creates a new clazz but it is linked and created in the outer context
  def runnerCompilation(outerctx: CodegenContext, terms: CompilerTerms, ctx: CodegenContext, codeBody: ExprCode,
                        ev: ExprCode, ruleSuiteId: VersionedId): (CodeAndComment, ExprCode) =
    runnerCompilation(outerctx, terms.parameterInformation,
      terms.runnerClassName, ctx, codeBody, ev, ruleSuiteId)

  // creates a new clazz but it is linked and created in the outer context
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
        /*(parameterInformation.copy(arity = parameterInformation.arity + 1,
          params = parameterInformation.params :+ ("InternalRow", ctx.INPUT_ROW, classOf[InternalRow]) // shouldn't have both either way, but just to be safe
        ),
          s"",//InternalRow ${ctx.INPUT_ROW}, ",
          s"",//${outerctx.INPUT_ROW}, ",
          "",
          "") */

    val id = s"${ruleSuiteId.id}_${ruleSuiteId.version}".replaceAll("-","__")

    // TODO maximum is 255 params, the codegenerator code has no upper limit, but it's 22 for function, need a array wrapper approach
    val runnerClassBody = s"""
      public RunnerCompilation$id generate(Object[] references) {
        return new RunnerCompilation$id(references);
      }

      class RunnerCompilation$id extends ${fullParams.aritySafeApplyType("scala.runtime.AbstractFunction")} {
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
    //v => s"$v = ($fun1) (($runnerClassName) references[$ruleRunnerExpressionIdx]).generatorClazz().generate(new Object[]{ references[$ruleRunnerExpressionIdx] });")
      v => s"$v = ($funX) (($runnerClassName) references[$ruleRunnerExpressionIdx]).generatorClazz().generate( references );")

    val res = ev.copy( code =
      code"""
        // push to top
        ${parameterInformation.pushToTop}
        // Call to RuleSuite Id(${ruleSuiteId.id},${ruleSuiteId.version})
        InternalRow ${ev.value} = (InternalRow) (($funX)$runner).apply($extraApplyParamCall ${fullParams.aritySafeParamCall});
        boolean ${ev.isNull} = false;
          """)

    (code, res)
  }

}

trait SplitCompilation {

  var generatorClassSource : CodeAndComment = _

  @transient
  var generatorClazz_ : GeneratedClass = _

  def generatorClazz: GeneratedClass = {
    // allow it to be replaced
    if (generatorClazz_ == null) {
      generatorClazz_ = CodeGenerator.compile(generatorClassSource)._1
    }
    generatorClazz_
  }

}

/**
  * Children will be rewritten by the plan, it's then re-incorporated into ruleSuite
  * expressionOffsets.length is the length of the trigger expressions in realChildren, realChildren(expressionOffsets.length + expressionOffsets(x)) will be the correct OutputExpression
  */
trait RuleEngineRunnerBase[T] extends NonSQLExpression with SplitCompilation {
  val ruleSuite: RuleSuite
  val compileEvals: Boolean
  val debugMode: Boolean
  val variablesPerFunc: Int
  val variableFuncGroup: Int
  val forceTriggerEval: Boolean
  val expressionOffsets: Array[Int]
  val userResultDataType: Option[DataType]
  val triggerCount: Int

  implicit val classTagT: ClassTag[T]

  lazy val resultDataType = {
    val resultDataType = outputExpressionType(userResultDataType, realChildren, triggerCount)

    if (debugMode)
      // wrap it in an array with the priority result
      ArrayType(StructType(Seq(StructField("salience", IntegerType), StructField("result", resultDataType))))
    else
      resultDataType
  }

  import RuleEngineRunnerUtils._

  lazy val realChildren = getRealChildren(children)

  // only used for compilation
  lazy val compiledRealChildren = realChildren.slice(0, triggerCount).map(ExpressionWrapper(_, compileEvals)).toArray

  override def nullable: Boolean = false
  override def toString: String = "RuleEngineRunner" + truncatedString(
    realChildren, "(", ", ", ")", SQLConf.get.maxToStringFields)

  // used only for eval, compiled uses the children directly TODO TEST ENGINE
  lazy val reincorporated = reincorporateExpressions(ruleSuite, realChildren, compileEvals, expressionOffsets, triggerCount)

  // keep it simple for this one. - can return an internal row or whatever..
  override def eval(input: InternalRow): Any = {
    val (res, rule, processedRes) = RuleSuiteFunctions.evalWithProcessors(reincorporated, input, debugMode)
    InternalRow(com.sparkutils.quality.impl.RuleRunnerUtils.ruleResultToRow(res),
      if (rule eq null) null else
      InternalRow(packId(rule._1),packId(rule._2),packId(rule._3)), processedRes)
  }

  def dataType: DataType = StructType( Seq(
      StructField(name = "ruleSuiteResults", dataType = impl.types.ruleSuiteResultType),
      StructField(name = "salientRule", dataType = impl.types.fullRuleIdType, nullable = true),
      StructField(name = "result", dataType = resultDataType, nullable = true)
    ))

  protected def doGenCodeI(outerCtx:  _root_.org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext, ev:  _root_.org.apache.spark.sql.catalyst.expressions.codegen.ExprCode): _root_.org.apache.spark.sql.catalyst.expressions.codegen.ExprCode = {

    outerCtx.references += this
    val ctx = QualityCodeGenUtils.clone(outerCtx)

    // #128 jump out of expr or rule groups
    val earlyReturn =
      (currRuleResTerm: String) =>
      code"""
        if ($currRuleResTerm == $PassedInt) {
          return;
        }
      """

    val compilerTerms =
      RuleEngineRunnerUtils.genCompilerTerms[T](outerCtx, ctx, PassThroughEvalOnly(realChildren),
        expressionOffsets, realChildren,
        debugMode, variablesPerFunc, variableFuncGroup, forceTriggerEval,
        exprEnd = earlyReturn, exprFunEnd = earlyReturn
      )

    import compilerTerms._
    import parameterInformation._

    // for debug currentOutputIndex is the count of matches, new Integer for #128 as janino isn't happy

    val pre = code"""
          $currentSalience = java.lang.Integer.MAX_VALUE;
          $currentOutputIndex = -1;
          $hasAPassTerm = false;
          // #128 enable early exit
          $currRuleResTerm = $UnevaluatedRuleInt;
          java.util.Arrays.fill((Object[])$resArrTerm, new Integer($UnevaluatedRuleInt));
          java.util.Arrays.fill($outArrTerm, null);

          ${funNames.map{f => s"$f($paramsCall);"}.mkString("\n")}
      """

    val resName = ctx.freshName("result")
    val resNull = ctx.freshName("isNull")

    val exp = ExprCode(VariableValue(resName, ev.value.javaType), isNullVariable(resNull))

    val post = code"""

          boolean ${exp.isNull} = false;
      """

    val res =
      if (debugMode)
        exp.copy(code = code"""
          $pre

          InternalRow ${exp.value} =
            com.sparkutils.quality.impl.RuleEngineRunnerUtils.compiledEvalDebug(
              $utilsName.evalArrayForDefault($ruleSuitTerm, $ruleSuiteArrays, $resArrTerm),
            ($currentOutputIndex < 0) ? null : com.sparkutils.quality.impl.RuleEngineRunnerUtils.debugOutput($salienceArrTerm, $outArrTerm, $currentOutputIndex, null));

          $post
          """
        )
      else
        exp.copy(code = code"""
          $pre

          InternalRow ${exp.value} =
            com.sparkutils.quality.impl.RuleEngineRunnerUtils.compiledEval(
              $utilsName.evalArrayForDefault($ruleSuitTerm, $ruleSuiteArrays, $resArrTerm),
              $currentSalience, $ruleTupleArrTerm, $currentOutputIndex, $outArrTerm);

          $post
          """
        )

    val (clazz, fres) = runnerCompilation(outerCtx, compilerTerms, ctx, res, ev, ruleSuite.id)
    generatorClassSource = clazz
    fres
  }
}

case class RuleEngineRunnerEval(ruleSuite: RuleSuite, children: Seq[Expression], userResultDataType: Option[DataType],
                            compileEvals: Boolean, debugMode: Boolean, variablesPerFunc: Int,
                            variableFuncGroup: Int, expressionOffsets: Array[Int],
                            forceTriggerEval: Boolean, triggerCount: Int) extends RuleEngineRunnerBase[RuleEngineRunnerEval] with CodegenFallback {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(children = newChildren)

  override implicit val classTagT: ClassTag[RuleEngineRunnerEval] = ClassTag(classOf[RuleEngineRunnerEval])

}


case class RuleEngineRunner(ruleSuite: RuleSuite, children: Seq[Expression], userResultDataType: Option[DataType],
                                compileEvals: Boolean, debugMode: Boolean, variablesPerFunc: Int,
                                variableFuncGroup: Int, expressionOffsets: Array[Int],
                                forceTriggerEval: Boolean, triggerCount: Int) extends RuleEngineRunnerBase[RuleEngineRunner] {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(children = newChildren)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = doGenCodeI(ctx, ev)

  override implicit val classTagT: ClassTag[RuleEngineRunner] = ClassTag(classOf[RuleEngineRunner])
}

