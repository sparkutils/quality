package com.sparkutils.quality.impl

import com.sparkutils.quality.impl.RuleRunnerUtils.packTheId
import com.sparkutils.quality.impl.util.{EmptyMap, IntegerArray, LongArray, ParameterInformation, RuleSetMap}
import com.sparkutils.quality.impl.util.ExtraConfig.ConfigMapOps
import com.sparkutils.quality.{FailedInt, PassedInt, RuleSuite, UnevaluatedRuleInt, classicFunctions, groupProcessorDumpAuditKey, showSplitCompilationTime, useEmptyRuleSetResults}
import com.sparkutils.testing.ConnectWhenForced.someOrForcedConnect
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{Block, CodeAndComment, CodeGenerator, CodegenContext, GeneratedClass}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Column, DataFrame}

import scala.concurrent.duration.Duration

/**
 * forwards to the server side implementations, when used in server, where stub is dropped, jar resolveWith can work
 */
object Runners {

  def ruleRunner(ruleSuite: RuleSuite, compileEvals: Boolean = false, resolveWith: Option[DataFrame] = None,
                 variablesPerFunc: Int = 40, variableFuncGroup: Int = 20, forceRunnerEval: Boolean = false,
                 extraConfig: Map[String, String] = Map.empty):
    Option[Column] = someOrForcedConnect(classicFunctions.ruleRunner(ruleSuite, compileEvals, resolveWith, variablesPerFunc,
      variableFuncGroup, forceRunnerEval, extraConfig))

  def ruleFolderRunner(ruleSuite: RuleSuite, startingStruct: Column, compileEvals: Boolean = false,
                       debugMode: Boolean = false, resolveWith: Option[DataFrame] = None, variablesPerFunc: Int = 40,
                       variableFuncGroup: Int = 20, forceRunnerEval: Boolean = false, useType: Option[StructType] = None,
                       forceTriggerEval: Boolean = false, extraConfig: Map[String, String] = Map.empty): Option[Column] =
    someOrForcedConnect(classicFunctions.ruleFolderRunner(ruleSuite, startingStruct, compileEvals, debugMode, resolveWith, variablesPerFunc,
      variableFuncGroup, forceRunnerEval, useType, forceTriggerEval, extraConfig))

  def ruleEngineRunner(ruleSuite: RuleSuite, resultDataType: Option[DataType] = None, compileEvals: Boolean = false,
                       debugMode: Boolean = false, resolveWith: Option[DataFrame] = None, variablesPerFunc: Int = 40,
                       variableFuncGroup: Int = 20, forceRunnerEval: Boolean = false, forceTriggerEval: Boolean = false,
                       extraConfig: Map[String, String] = Map.empty):
    Option[Column] = someOrForcedConnect(classicFunctions.ruleEngineRunner(ruleSuite, resultDataType, compileEvals, debugMode,
      resolveWith, variablesPerFunc, variableFuncGroup, forceRunnerEval, forceTriggerEval, extraConfig))

}

/**
 * Used for compilation
 * @param offsets each trigger offset
 * @param beforeProcessing must be placed in the generated code after the output row is copied and sets up the
 *                         class variables for each ruleSet
 * @param resultRowPrep must be called after processing and re-integrates the overallResult's from each set and suite
 */
case class InPlaceOffsets(offsets: Seq[String => Block], beforeProcessing: Block, resultRowPrep: Block,
                          runner: String, runnerClazz: Class[_])

/**
 * Shared trait for all runners
 */
trait Runner extends Expression {

  /**
   * After calling now wrapping of zero code will be performed when the [[com.sparkutils.quality.impl.extension.ZeroCodeGenWrap]]
   * optimisation is enabled
   * @return
   */
  def withZeroCode(): Runner

  val alreadyZero: Boolean

  def ruleSuite: RuleSuite
  val extraConfig: Map[String, String]
  val variablesPerFunc: Int
  val variableFuncGroup: Int

  val defaultRuleResult: Int
  val defaultOverallResult: Int

  val defaultOverallProcessor: (Int, Int) => Int

  /**
   * Used by codegen
   */
  def createDefaultRuleResult(): InternalRow =
    InternalRow(packTheId(ruleSuite.id), defaultOverallResult,
      if (extraConfig.boolean(useEmptyRuleSetResults, false))
        EmptyMap  // copy is expensive
      else {

        val ids = new LongArray( ruleSuite.ruleSets.map{ ruleSet => packTheId(ruleSet.id) }.toArray )
        val rules = ruleSuite.ruleSets.map{
          ruleSet =>
            InternalRow(defaultOverallResult, {
              val ids = ruleSet.rules.map(r => packTheId(r.id)).toArray
              val defaults = ruleSet.rules.map( _ => defaultRuleResult).toArray
              // use optimised copys, by default generic array does scanning
              new RuleSetMap(
                new LongArray(ids),
                new IntegerArray(defaults)
              )
            })
        }

        new ArrayBasedMapData(ids, new GenericArrayData(rules.toArray))
      }
    )

  /**
   * rolls the overalls up in place - must be genericarraydata / arraybasedmap data with a copy from createDefaultRuleResult
   * used by codegen
   */
  final def applyResult(level1: Int, level2: Int, result: InternalRow, ruleResult: Int): Unit = {
    val sar = result.getMap(2).asInstanceOf[ArrayBasedMapData]
    // update result directly
    val sv = sar.valueArray.asInstanceOf[GenericArrayData]
    val struct = sv.getStruct(level1, 2)
    struct.getMap(1).valueArray().asInstanceOf[IntegerArray].update(level2, ruleResult) // comment out to squeeze extra, spark still more expensive by 20s, nothing in quality code

    // processOverall
    val cur = struct.getInt(0)
    val nr = defaultOverallProcessor(ruleResult, cur)
    struct.update(0, nr)
    result.update(1, defaultOverallProcessor(nr, result.getInt(1)))
  }

  /**
   * only applies overallResult to the top level result and ignores any rule or ruleset level information
   * used by codegen
   */
  final def applyEmptyResult(level1: Int, level2: Int, result: InternalRow, ruleResult: Int): Unit = {
    result.update(1, defaultOverallProcessor(ruleResult, result.getInt(1)))
  }

  def applyNonEmptySuffix: String = ""

  def inPlaceArrayOffsets(ctx: CodegenContext, resultRow: String, ruleRunnerExpressionIdx: Int): InPlaceOffsets = {
    val className = this.getClass.getName
    val runner = ctx.freshName("runner")
    ctx.addImmutableStateIfNotExists(className, runner,
      v => s"$v = (($className)references[$ruleRunnerExpressionIdx]);")

    val (empty, suffix) =
      if (extraConfig.boolean(useEmptyRuleSetResults, false))
        ("Empty", "")
      else
        ("", applyNonEmptySuffix)

    InPlaceOffsets(ruleSuite.ruleSets.zipWithIndex.flatMap{
      case (ruleSet, level1) =>
        ruleSet.rules.zipWithIndex.map{
          case (_, level2) =>
            (result: String) =>
              code"""
               $runner.apply${empty}Result${suffix}($level1, $level2, $resultRow, $result);
                """
        }
    }, code"", code"", runner, this.getClass)
  }

}

/**
 * Base trait for dq and expression
 */
trait TriggerOnly extends Runner {
  val defaultOverallProcessor: (Int, Int) => Int = OverallResultHelper.inplaceInt(_,_, ruleSuite.probablePass)
  val defaultRuleResult: Int = PassedInt
  val defaultOverallResult: Int = PassedInt
}

/**
 * Base trait for runners that use triggers_ collector, engine and folder
 */
trait HasOutput extends Runner {

  val defaultRuleResult: Int = UnevaluatedRuleInt
  val defaultOverallResult: Int = FailedInt
  val defaultOverallProcessor: (Int, Int) => Int = OverallResultHelper.inplaceForDefaultInt(_,_, ruleSuite.probablePass)

  val triggerCount: Int

  def realChildren: Seq[Expression]

  def triggerRules: Seq[Expression] = realChildren.slice(0, triggerCount)

  def canAudit: Boolean = triggerRules.forall(_.resolved)

  /**
   * For collector and engine it's typically just their sql function name, for folder
   * it must also include the starting expression.sql.  This will be called with resolved
   * expressions but not with structs so the folder starter expression is expected to be
   * executable
   * @param ruleSuiteCall provided by the grouping code but resolves to a rulesuite
   * @return
   */
  def groupedSqlCall(ruleSuiteCall: String): String

  private lazy val shouldAudit = extraConfig.boolean(groupProcessorDumpAuditKey, false)

  val audited: Boolean

  def performGroupingAuditDump(): Unit = {
    if (shouldAudit && canAudit) {

      Triggers.loadTriggerGrouper(extraConfig).dumpAudit(this)

    }
  }
}

/**
 * Handles state management for separate compilation, the generated source code is cached locally and then compiled on
 * demand
 */
trait SplitCompilation extends Runner {

  var generatorClassSource : Map[Int, CodeAndComment] = _

  @transient
  lazy val generatorClazz_ : Map[Int, GeneratedClass] = {
    val start = System.nanoTime()

    val generatorClazz = generatorClassSource.map{b => b._1 -> CodeGenerator.compile(b._2)._1}

    val end = System.nanoTime()
    val compileTime = Duration.fromNanos(end - start)
    if (extraConfig.boolean(showSplitCompilationTime, false)){
      println(s"${this.getClass.getSimpleName} RuleSuite ${ruleSuite.id} - took ${compileTime.toMinutes}m${compileTime.toSeconds % 60}s to compile")
    }
    generatorClazz
  }

  // used by compilation
  def generatorClazz(i: Int): GeneratedClass = {
    // allow it to be replaced
    generatorClazz_(i)
  }

  def setClazzSource(seq: Seq[(Int, CodeAndComment)]): Unit = {
    /*this match {
      case h: HasOutput => println(" children --- >" + h.realChildren)
    }
    seq.foreach(p => println(p._1 + " --> " + p._2.body))*/
    generatorClassSource = seq.toMap
  }

  @transient
  var usedParameters_ : ParameterInformation = _

  def setUsedParameters(parameters: ParameterInformation) = {
    usedParameters_ = parameters
  }
}
