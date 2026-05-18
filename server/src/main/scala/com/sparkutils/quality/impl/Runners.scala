package com.sparkutils.quality.impl

import com.sparkutils.quality.impl.RuleRunnerUtils.packTheId
import com.sparkutils.quality.{FailedInt, PassedInt, RuleSuite, UnevaluatedRuleInt, classicFunctions, groupProcessorAuditKey}
import com.sparkutils.testing.ConnectWhenForced.someOrForcedConnect
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{Block, CodegenContext}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Column, DataFrame}

import scala.util.Try

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

/* TODO
the processOverall needs to be a runner level function,
eavh ruleset needs the array and the struct in vars
the processOverall should be done on class variables only then inserted
two functions, one to reset the 2/3 vars per set and one to integrate them again
then at codegen site just array updates are used via level2 for result storage and process via the class vars
potentially need to create a java version of the processsOverall logic.
 */
case class InPlaceOffset(level1: Int, level2: Int) {
  /**f
   * rolls the overalls up in place - must be genericarraydata / arraybasedmap data with a copy from createDefaultRuleResult
   */
  def applyResult(result: InternalRow, ruleResult: Int, processOverall: (Int, Int) => Int): Unit = {
    val sar = result.getMap(2).asInstanceOf[ArrayBasedMapData]
    // update result directly
    val sv = sar.valueArray.asInstanceOf[GenericArrayData]
    val struct = sv.getStruct(level1, 2)
    struct.getMap(1).valueArray.asInstanceOf[GenericArrayData].update(level2, ruleResult)

    // processOverall
    val cur = struct.getInt(0)
    val nr = processOverall(ruleResult, cur)
    struct.update(0, nr)
    result.update(1, processOverall(nr, result.getInt(1)))
  }
  /**
   * only for expression runner
   */
  def applyExpression(result: InternalRow, ruleResult: Any): Unit = {
    val sar = result.getMap(2).asInstanceOf[ArrayBasedMapData]
    // update result directly
    val sv = sar.valueArray.asInstanceOf[GenericArrayData]
    val struct = sv.getStruct(level1, 2)
    struct.getMap(1).valueArray.asInstanceOf[GenericArrayData].update(level2, ruleResult)
  }
}

/**
 * Used for compilation
 * @param offsets each trigger offset
 * @param beforeProcessing must be placed in the generated code after the output row is copied and sets up the
 *                         class variables for each ruleSet
 * @param resultRowPrep must be called after processing and re-integrates the overallResult's from each set and suite
 */
case class InPlaceOffsets(offsets: Seq[String => Block], beforeProcessing: Block, resultRowPrep: Block)

/**
 * Shared trait for all runners
 */
trait Runner extends Expression {

  def ruleSuite: RuleSuite
  val extraConfig: Map[String, String]
  val variablesPerFunc: Int
  val variableFuncGroup: Int

  val defaultRuleResult: Int
  val defaultOverallResult: Int

  val defaultOverallProcessor: (Int, Int) => Int

  /**
   * Used by compilation
   * @return
   */
  def createDefaultRuleResult(): InternalRow =
    InternalRow(packTheId(ruleSuite.id), defaultOverallResult,
      ArrayBasedMapData(
        ruleSuite.ruleSets.map{
          ruleSet =>
            packTheId(ruleSet.id) -> InternalRow(defaultOverallResult,
              ArrayBasedMapData(
                ruleSet.rules.map( r => packTheId(r.id) -> defaultRuleResult).toMap
              ))
        }.toMap
      )
    )

    /**
     * rolls the overalls up in place - must be genericarraydata / arraybasedmap data with a copy from createDefaultRuleResult
     */
    def applyResult(level1: Int, level2: Int, result: InternalRow, ruleResult: Int): Unit = {
      val sar = result.getMap(2).asInstanceOf[ArrayBasedMapData]
      // update result directly
      val sv = sar.valueArray.asInstanceOf[GenericArrayData]
      val struct = sv.getStruct(level1, 2)
      struct.getMap(1).valueArray.asInstanceOf[GenericArrayData].update(level2, ruleResult)

      // processOverall
      val cur = struct.getInt(0)
      val nr = defaultOverallProcessor(ruleResult, cur)
      struct.update(0, nr)
      result.update(1, defaultOverallProcessor(nr, result.getInt(1)))
    }

    def inPlaceArrayOffsets(ctx: CodegenContext, resultRow: String, ruleRunnerExpressionIdx: Int): InPlaceOffsets = {
      val className = this.getClass.getName
      val runner = ctx.freshName("runner")
      ctx.addImmutableStateIfNotExists(className, runner,
        v => s"$v = (($className)references[$ruleRunnerExpressionIdx]);")

      InPlaceOffsets(ruleSuite.ruleSets.zipWithIndex.flatMap{
        case (ruleSet, level1) =>
          ruleSet.rules.zipWithIndex.map{
            case (_, level2) =>
              result =>
                code"""
                 $runner.applyResult($level1, $level2, $resultRow, $result);
                  """
          }
      }, code"", code"")
    }
    def inPlaceArrayOffsetsB(ctx: CodegenContext, resultRow: String, ruleRunnerExpressionIdx: Int): InPlaceOffsets = {
      val className = this.getClass.getName

      val currentSetOverall = ctx.addMutableState("int[]", "currentSetOverall",
        v => s"$v = new int[${ruleSuite.ruleSets.size}];")
      val currentOverall = ctx.addMutableState("int", "currentOverall")
      val setArray = ctx.addMutableState("Object[][]", "setArray",
        v => s"$v = new Object[${ruleSuite.ruleSets.size}][];")
      val processOverall = ctx.freshName("processOverall")
      ctx.addImmutableStateIfNotExists("scala.Function2<Integer,Integer,Integer>", processOverall,
        v => s"$v = ((($className)references[$ruleRunnerExpressionIdx]).defaultOverallProcessor());")

      val sar = ctx.freshName("sar")
      val sv = ctx.freshName("sv")
      val struct = ctx.freshName("struct")

      val gad = "org.apache.spark.sql.catalyst.util.GenericArrayData"
      val abd = "org.apache.spark.sql.catalyst.util.ArrayBasedMapData"

      InPlaceOffsets(ruleSuite.ruleSets.zipWithIndex.flatMap{
        case (ruleSet, level1) =>
          ruleSet.rules.zipWithIndex.map{
            case (_, level2) =>
              result =>
                code"""
                 $setArray[$level1][$level2] = (Integer) $result;

                  if ($result != $currentSetOverall[$level1]) {
                   $currentSetOverall[$level1] = (Integer) $processOverall.apply((Integer)$result, (Integer)$currentSetOverall[$level1]);
                   $currentOverall =  (Integer) $processOverall.apply((Integer)$currentSetOverall[$level1], (Integer)$currentOverall);
                 }
                 """
          }
        }, {
        code"""
           $abd $sar = ($abd)$resultRow.getMap(2);
           $gad $sv = ($gad)$sar.valueArray();
           InternalRow $struct = null;
           $currentOverall = $resultRow.getInt(1);
           ${
             ruleSuite.ruleSets.zipWithIndex.map {
               case (_, index) =>
                 s"""
                   $struct = $sv.getStruct($index, 2);
                   $currentSetOverall[$index] = $struct.getInt(0);
                   $setArray[$index] = (Object[]) ( ($gad)( ($abd) $struct.getMap(1) ).valueArray() ).array();"""
             }.mkString("\n")
           }
          """
        },
        code"""
           ${
            ruleSuite.ruleSets.zipWithIndex.map {
              case (_, index) =>
                s"""
                $struct = $sv.getStruct($index, 2);
                $struct.update(0,$currentSetOverall[$index]);
                """
              }.mkString("\n")
            }
           $resultRow.update(1, $currentOverall);
          """
      )

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

  private lazy val shouldAudit = Try(Triggers.getValue(groupProcessorAuditKey, extraConfig, "false").toBoolean).getOrElse(false)

  val audited: Boolean

  def performGroupingAuditDump(): Unit = {
    if (shouldAudit && canAudit) {

      Triggers.loadTriggerGrouper(extraConfig).dumpAudit(this)

    }
  }
}
