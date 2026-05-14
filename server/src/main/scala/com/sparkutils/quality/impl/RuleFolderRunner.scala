package com.sparkutils.quality.impl

import com.sparkutils.quality.{impl, _}
import com.sparkutils.quality.impl.GetRealChildren.getRealChildren
import com.sparkutils.quality.impl.imports.ClassicRuleFolderRunnerImports
import com.sparkutils.quality.impl.util.{PassThroughEvalOnly, SeparateCompilation}
import com.sparkutils.quality.impl.util.SeparateCompilation.runnerCompilation
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.JavaCode.isNullVariable
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode, QualityCodeGenUtils, VariableValue}
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.qualityFunctions.{FunN, RefExpressionLazyType}
import org.apache.spark.sql.types._

import java.util.concurrent.atomic.AtomicReference
import scala.reflect.ClassTag

private[quality] object RuleFolderRunnerUtils extends ClassicRuleFolderRunnerImports {

  /**
   * Needs sorting in salience order for output processing but NOT for result's.
   * So output array should use offsets, results should be offset, but order
   * of calling must be salience based.
   *
   */

  def compiledEvalDebug[T](results: InternalRow, output: T): InternalRow =
    InternalRow(results, output)

  def compiledEval[T](results: InternalRow, currentSalience: Int, rules: Array[(Long, Long, Long)],
                      currentOutputIndex: Int, output: Array[T], default: T): InternalRow =
    InternalRow(results,
      if (currentSalience == java.lang.Integer.MAX_VALUE && default == null)
        null
      else
        if (default != null)
          default
        else
          output(currentOutputIndex)
    )

}

/**
  * Children will be rewritten by the plan, it's then re-incorporated into ruleSuite
  * expressionOffsets.length is the length of the trigger expressions in realChildren, realChildren(expressionOffsets.length + expressionOffsets(x)) will be the correct OutputExpression
  */
trait RuleFolderRunnerBase[T] extends NonSQLExpression with SplitCompilation {

  val ruleSuite: RuleSuite
  val resultDataType: () => DataType
  val compileEvals: Boolean
  val debugMode: Boolean
  val variablesPerFunc: Int
  val variableFuncGroup: Int
  val expressionOffsets: Array[Int]
  val dataRef: AtomicReference[DataType]
  val forceTriggerEval: Boolean
  val triggerCount: Int
  val extraConfig: Map[String, String]

  implicit val classTagT: ClassTag[T]
  val tClass: Class[T]

  import RuleEngineRunnerUtils._
  import RuleFolderRunnerUtils._

  val startingStruct: Expression = children.head
  // resolution only happens on driver.  Only set once or the plan can get different nullables on lower sparks
  if (startingStruct.resolved) {
    val cur = dataRef.get()
    if (cur eq null) {
      dataRef.set(startingStruct.dataType)
    }
  }

  lazy val realChildren = getRealChildren(children.tail)

  // only used for compilation
  lazy val compiledRealChildren = realChildren.slice(0, triggerCount).map(ExpressionWrapper(_, compileEvals)).toArray

  override def nullable: Boolean = false
  override def toString: String = "RuleFolderRunner" + truncatedString(
    children, "(", ", ", ")", SQLConf.get.maxToStringFields)

  // used only for eval, compiled uses the children directly
  lazy val reincorporated = reincorporateExpressions(ruleSuite, realChildren, compileEvals, expressionOffsets, triggerCount)

  // keep it simple for this one. - can return an internal row or whatever..
  override def eval(input: InternalRow): Any = {
    val starter = startingStruct.eval(input).asInstanceOf[InternalRow] // TODO - throw a decent error message at ruleFolder call
    val (res, processedRes) = //(null, null)
      RuleSuiteFunctions.foldWithProcessors(reincorporated, input, starter, debugMode)
    InternalRow(com.sparkutils.quality.impl.RuleRunnerUtils.ruleResultToRow(res), processedRes)
  }

  def dataType: DataType = StructType( Seq(
      StructField(name = "ruleSuiteResults", dataType = impl.types.ruleSuiteResultType),
      StructField(name = "result", dataType = resultDataType(), nullable = true)
    ))

  protected def doGenCodeI(outerCtx:  _root_.org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext, ev:  _root_.org.apache.spark.sql.catalyst.expressions.codegen.ExprCode): _root_.org.apache.spark.sql.catalyst.expressions.codegen.ExprCode = {

    val (clazz, fres) = SeparateCompilation.withSubExpressions(this, realChildren, outerCtx, ev, ruleSuite.id) {
      (ctx, ruleRunnerExpressionIdx) =>

        // need to setup the folder variable to pass around, create it with "left"
        // thread it through
        val folderV = ctx.addMutableState( "InternalRow",
          ctx.freshName("folderV") )

        def hasDefault(when: => String, els: String = ""): String =
          if (ruleSuite.defaultProcessor != NoOpDefaultProcessor.noOp)
            when
          else
            els

        val sizeAdjustment =
          if (ruleSuite.defaultProcessor != NoOpDefaultProcessor.noOp)
            -1 // don't generate the default, there isn't a trigger
          else
            0

        // order by salience
        val salience = com.sparkutils.quality.impl.RuleEngineRunnerUtils.flattenSalience(ruleSuite)
        val reordered = // fill the index list, still only uniques
          (0 until triggerCount).map{i =>
            // lookup the output expressions
            expressionOffsets(i)
          } zip salience sortBy(_._2) map(_._1)

        val lazyRefsGenCode = realChildren.drop(triggerCount).map(_.asInstanceOf[FunN].arguments.head.genCode(ctx))

        val compilerTerms =
          RuleEngineRunnerUtils.genCompilerTerms[T](ruleRunnerExpressionIdx, outerCtx, ctx, PassThroughEvalOnly(realChildren), expressionOffsets, realChildren,
            debugMode, variablesPerFunc, variableFuncGroup, forceTriggerEval, extraConfig,
            // capture the current
            extraResult = (outArrTerm: String, _, _) => s"$folderV = $outArrTerm;",
            extraSetup = (_, i: Int) =>
              s"""
          // set the current row for the fold for flattened rule $i
          ${lazyRefsGenCode(i).value} = $folderV;
          ${lazyRefsGenCode(i).isNull} = $folderV == null;
          """,
            orderOffset = (idx: Int) => reordered(idx),
            // we shouldn't check salience as we are already ordered by it
            salienceCheck = false,
            sizeAdjustment = sizeAdjustment
          )

        import compilerTerms._
        import parameterInformation._

        // generate the starting struct
        val starterEval = startingStruct.genCode(ctx)

        val rsres = ctx.freshName("ruleSuiteRes")
        val default = ctx.freshName("defaultRes")

        val pre = code"""
          $currentSalience = java.lang.Integer.MAX_VALUE;
          $currentOutputIndex = -1;
          $hasAPassTerm = false;

          // starting
          ${starterEval.code}
          // setting the folder
          $folderV = ${starterEval.isNull} ? null : (InternalRow)${starterEval.value}; \n

          ${funNames.map{f => s"$f($paramsCall);"}.mkString("\n")}

          InternalRow $rsres = $utilsName.evalArrayForDefault($ruleSuitTerm, $ruleSuiteArrays, $resArrTerm);
          InternalRow $default = null;

         ${hasDefault {
          s"""
            if (!$hasAPassTerm) {
            ${
            val defP = realChildren.last.genCode(ctx)
            s"""
                  ${lazyRefsGenCode.last.value} = $folderV;
                  ${lazyRefsGenCode.last.isNull} = $folderV == null;
                  ${defP.code}

                  // System.out.println("DefaultProcessor result is ${defP.value}" + ${defP.value});
                  $default = ${defP.value};

                  $rsres.update(1, ${DefaultRuleInt});
                """
          }
            }
            """ }
        }
      """

        val resName = ctx.freshName("result")
        val resNull = ctx.freshName("isNull")

        val exp = ExprCode(VariableValue(resName, ev.value.javaType), isNullVariable(resNull))

        val post = s"""

          boolean ${exp.isNull} = false;
      """

        val res =
          if (debugMode)
            exp.copy(code = code"""
          $pre

          InternalRow ${exp.value} =
            com.sparkutils.quality.impl.RuleFolderRunnerUtils.compiledEvalDebug($rsres,
             (($currentOutputIndex < 0) && ($default == null)) ? null :
              com.sparkutils.quality.impl.RuleEngineRunnerUtils.debugOutput($salienceArrTerm, $outArrTerm, $currentOutputIndex, $default));

          $post
          """
            )
          else
            exp.copy(code = code"""
          $pre

          InternalRow ${exp.value} =
            com.sparkutils.quality.impl.RuleFolderRunnerUtils.compiledEval($rsres,
              $currentSalience, $ruleTupleArrTerm, $currentOutputIndex, $outArrTerm, $default);

          $post
          """
            )
        (compilerTerms, res)
    }

    generatorClassSource = clazz
    fres
  }

  def processNewChildren(newChildren: Seq[Expression]): Seq[Expression] = {
    val starter = newChildren.head
    val rest = newChildren.tail
    val crest =
      if (starter.resolved) {

        rest.map(_.transform{
          case r@ RefExpressionLazyType(_, _, false) =>
            r.copy(_resolved = true)
        })

      } else
        rest

    starter +: crest
  }

}


/**
 * Children will be rewritten by the plan, it's then re-incorporated into ruleSuite
 * expressionOffsets.length is the length of the trigger expressions in realChildren, realChildren(expressionOffsets.length + expressionOffsets(x)) will be the correct OutputExpression
 */
case class RuleFolderRunnerEval(ruleSuite: RuleSuite, children: Seq[Expression], resultDataType: () => DataType,
                            compileEvals: Boolean, debugMode: Boolean, variablesPerFunc: Int,
                            variableFuncGroup: Int, expressionOffsets: Array[Int],
                            dataRef: AtomicReference[DataType], forceTriggerEval: Boolean,
                            triggerCount: Int, extraConfig: Map[String, String]
                           ) extends RuleFolderRunnerBase[RuleFolderRunnerEval] with CodegenFallback {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = processNewChildren(newChildren))

  override implicit val classTagT: ClassTag[RuleFolderRunnerEval] = ClassTag(classOf[RuleFolderRunnerEval])

  override val tClass: Class[RuleFolderRunnerEval] = classOf[RuleFolderRunnerEval]
}


/**
 * Children will be rewritten by the plan, it's then re-incorporated into ruleSuite
 * expressionOffsets.length is the length of the trigger expressions in realChildren, realChildren(expressionOffsets.length + expressionOffsets(x)) will be the correct OutputExpression
 */
case class RuleFolderRunner(ruleSuite: RuleSuite, children: Seq[Expression], resultDataType: () => DataType,
                            compileEvals: Boolean, debugMode: Boolean, variablesPerFunc: Int,
                            variableFuncGroup: Int, expressionOffsets: Array[Int],
                            dataRef: AtomicReference[DataType], forceTriggerEval: Boolean,
                            triggerCount: Int, extraConfig: Map[String, String]
                               ) extends RuleFolderRunnerBase[RuleFolderRunner] {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = processNewChildren(newChildren))

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = doGenCodeI(ctx, ev)

  override implicit val classTagT: ClassTag[RuleFolderRunner] = ClassTag(classOf[RuleFolderRunner])
  override val tClass: Class[RuleFolderRunner] = classOf[RuleFolderRunner]
}
