package com.sparkutils.quality.impl

import com.sparkutils.quality._
import com.sparkutils.quality.impl.CollectRunner.UnrollOutputArraySize
import com.sparkutils.quality.impl.RuleEngineRunnerUtils.{flattenExpressions, outputExpressionType}
import com.sparkutils.quality.impl.extension.ZeroCodeGenWrap
import com.sparkutils.quality.impl.imports.RuleFolderRunnerImports
import com.sparkutils.quality.impl.util.{GenerateResult, PassThroughEvalOnly, SeparateCompilation}
import com.sparkutils.quality.impl.util.SeparateCompilation.runnerCompilation
import com.sparkutils.shim.expressions.Names
import org.apache.spark.sql.Column
import org.apache.spark.sql.ShimUtils.column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.JavaCode.isNullVariable
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral, GlobalValue, QualityCodeGenUtils, VariableValue}
import org.apache.spark.sql.catalyst.expressions.{CreateArray, Expression, NonSQLExpression}
import org.apache.spark.sql.catalyst.util.{GenericArrayData, truncatedString}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


private[quality] object CollectRunnerUtils extends RuleFolderRunnerImports {

  def compiledEval[T](results: InternalRow, output: ArrayBuffer[T]): InternalRow =
    InternalRow(results, new GenericArrayData(output))

}

/**
 * Replacement for CreateArray when flatten is true, eval creates new arrays, but gencode does not and only replaces the array location
 * @param children
 */
case class InPlaceArray(children: Seq[Expression]) extends Expression {

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any =
    new GenericArrayData(children.map(_.eval(input)).toArray)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val ruleRes = "java.lang.Object"
    val arrayTerm = ctx.addMutableState(ruleRes+"[]", ctx.freshName("results"),
      v => s"$v = new $ruleRes[${children.size}];")
    val genArr = classOf[GenericArrayData].getName
    val arrayDataTerm = ctx.addMutableState(genArr, ctx.freshName("retval"),
      v => s"$v = new $genArr($arrayTerm);")

    val theCode = children.zipWithIndex.map {
      case (child, i) =>

        val eval = child.genCode(ctx)
        // TODO will autoboxing work on databricks? it's had an old janino version for a long time - tests need
        s"""
          // InPlaceArray for elem $i
          ${eval.code}

          $arrayTerm[$i] = ${eval.value};
        """
    }


    val splitCode = ctx.splitExpressionsWithCurrentInputs(
      expressions = theCode,
      funcName = "inPlaceApplyAD",
      extraArguments = (ruleRes+"[]", arrayTerm) :: Nil)

    ev.copy(code =
      code"""
        $splitCode
        """, isNull = FalseLiteral, value = GlobalValue(arrayDataTerm, classOf[GenericArrayData]))

  }

  override def dataType: ArrayType = {
    ArrayType(
      outputExpressionType(None, children, 0),
      containsNull = children.exists(_.nullable))
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(newChildren)
}

object CollectRunner {

  /**
   * defaults to true, swapping out array( / CreateArray usage to reduce array creation and have direct array access
   */
  val UseInPlaceArray = "com.sparkutils.collect.useInPlaceArray"
  /**
   * defaults to false, enables output expressions with InPlaceArray to have unrolling applied,
   * the default unrollOutputArraySize of 1 behaves like for loop.  In testing for small array output expression sizes
   * there is a loss of performance for unrolling.  It is likely only useful for large "array(" size output expressions.
   * Up to 4 tested have shown zero benefit.
   */
  val UnrollOutputArray = "com.sparkutils.collect.unrollOutputArray"
  /**
   * defaults to 1, used to configure InPlaceArray unrolling, 1 behaves like a for loop, every higher number groups into
   * a for loop with unrollOutputArraySize entries and any leftover being directly unrolled.
   */
  val UnrollOutputArraySize = "com.sparkutils.collect.unrollOutputArraySize"

  /**
   * Creates a column that runs the folding RuleSuite.  This also forces registering the lambda functions used by that RuleSuite.
   *
   * FolderRunner runs all output expressions for matching rules in order of salience, the startingStruct is passed ot the first
   * matching, the result passed to the second etc.  In contrast to ruleEngineRunner OutputExpressions should be lambdas with one parameter, that of the structure
   *
   * By default, InPlaceArray will substitute CreateArray (array sql function), should there be issues with loop unrolling com.sparkutils.collect.useInPlaceArray can be set to false.
   *
   * @param ruleSuite The ruleSuite with runOnPassProcessors
   * @param resultDataType the collected result type, specify this if the derived types have nullability or ordering issues.  By default, it takes the type of the last output expression
   * @param variablesPerFunc Defaulting to 40 allows, in combination with variableFuncGroup allows customisation of handling the 64k jvm method size limitation when performing WholeStageCodeGen
   * @param variableFuncGroup Defaulting to 20
   * @param flatten when resultType is an ArrayType should the result be flattened
   * @param includeNulls should nulls returned by the output expressions be included, note when flattening nulls IN the returned arrays are not filtered
   * @return A Column representing the QualityRules expression built from this ruleSuite
   */
  def collectRunnerClassic(ruleSuite: RuleSuite, resultDataType: Option[DataType] = None, variablesPerFunc: Int = 40,
                           variableFuncGroup: Int = 20, flatten: Boolean = true, includeNulls: Boolean = false,
                           useInPlaceArray: Boolean = true, unrollInPlaceArray: Boolean = false,
                           unrollOutputArraySize: Int = 1, extraConfig: Map[String, String] = Map.empty): Column = {
    com.sparkutils.quality.registerLambdaFunctions( ruleSuite.lambdaFunctions )

    val (expressionsRaw, indexes, triggerCount) = flattenExpressions(ruleSuite)

    val cleaned = RuleLogicUtils.cleanExprs(ruleSuite)

    val inPlace = getConfig(UseInPlaceArray, s"$useInPlaceArray").toBoolean
    val unroll = getConfig(UnrollOutputArray, s"$unrollInPlaceArray").toBoolean

    val canUnroll =
      expressionsRaw.drop(triggerCount).map{
        case a: UnresolvedFunction if Names.toName(a).toLowerCase == "array" => a.children.size
        case _ => -1
      }.toArray

    val expressions =
      if (flatten && inPlace)
        expressionsRaw.zipWithIndex.map{
          case (a: UnresolvedFunction, i) if // check triggerCount because we do not want triggers to be swapped
            Names.toName(a).toLowerCase == "array" && i >= triggerCount => InPlaceArray(a.children) // TODO should this move into the expression and auto resolve in the case of FunNRewrite?
          case (e, i) => e
        }
      else
        expressionsRaw

    val isInPlace =
      expressions.drop(triggerCount).map{
        case _: InPlaceArray => true
        case _ => false
      }.toArray

    column(
      ZeroCodeGenWrap.wrap(
      CollectRunnerRunner(cleaned, expressions, resultDataType,
        variablesPerFunc, variableFuncGroup,
        expressionOffsets = indexes, triggerCount = triggerCount, flatten = flatten,
        includeNulls = includeNulls, canUnroll = canUnroll, isInPlace = isInPlace, unroll = unroll,
        unrollOutputArraySize = unrollOutputArraySize, extraConfig = extraConfig)
      )
    )
  }
}

/**
  * Children will be rewritten by the plan, it's then re-incorporated into ruleSuite
  * expressionOffsets.length is the length of the trigger expressions in realChildren, realChildren(expressionOffsets.length + expressionOffsets(x)) will be the correct OutputExpression
  */
trait CollectRunnerBase[T] extends Expression with NonSQLExpression with SplitCompilation with HasOutput {

  def groupedSqlCall(ruleSuiteCall: String): String = s"collect_runner($ruleSuiteCall)"

  def realChildren: Seq[Expression] = children

  val ruleSuite: RuleSuite
  val resultDataType: Option[DataType]
  val variablesPerFunc: Int
  val variableFuncGroup: Int
  val expressionOffsets: Array[Int]
  val flatten: Boolean
  val includeNulls: Boolean
  val triggerCount: Int
  val canUnroll: Array[Int]
  val unroll: Boolean
  val isInPlace: Array[Boolean]
  val unrollOutputArraySize: Int
  val extraConfig: Map[String, String]

  implicit val classTagT: ClassTag[T]
  val tClass: Class[T]

  import RuleEngineRunnerUtils._

  lazy val actualType = outputExpressionType(resultDataType, children, triggerCount)

  // only used for compilation compatibility with ruleEngine utils code
  lazy val compiledRealChildren = Array.empty[ExpressionWrapper]

  lazy val canFlatten = actualType.isInstanceOf[ArrayType]
  lazy val elementType: DataType = if (!canFlatten) null else actualType.asInstanceOf[ArrayType].elementType

  // e.g. starter space for each rule with 5 possible rows when flattening or exact size when it's an array
  // to find out the exact size after casting etc. involves a second pass and is measurably more expensive
  lazy val starterSize = {
    val initial = ruleSuite.ruleSets.map(_.rules.size).sum

    if (flatten && canFlatten) {
      // if array
      children.foldLeft(initial) { case (cur, e) =>
         e match {
           case a: CreateArray => a.children.size + cur
           case a: InPlaceArray => a.children.size + cur
           case _ => cur + 5 // number carefully picked from thin air
         }
      }
    } else
      initial
  }

  override def nullable: Boolean = false
  override def toString: String = classTagT.runtimeClass.getName + truncatedString(
    children, "(", ", ", ")", SQLConf.get.maxToStringFields)

  // used only for eval, compiled uses the children directly
  lazy val reincorporated = reincorporateExpressions(ruleSuite, children, false, expressionOffsets, triggerCount)

  override def eval(input: InternalRow): Any = {
    val (res, processedRes) = //(null, null)
      RuleSuiteFunctions.collect(reincorporated, input, flatten && canFlatten, includeNulls,
        elementType, starterSize)
    InternalRow(com.sparkutils.quality.impl.RuleRunnerUtils.ruleResultToRow(res), processedRes)
  }

  def dataType: DataType = StructType( Seq(
      StructField(name = "ruleSuiteResults", dataType = impl.types.ruleSuiteResultType),
      StructField(name = "result", dataType =
        if (flatten && canFlatten) ArrayType(elementType, includeNulls) else ArrayType(actualType, includeNulls),
        nullable = true)
    ))

  protected def doGenCodeI(outerCtx:  _root_.org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext, ev:  _root_.org.apache.spark.sql.catalyst.expressions.codegen.ExprCode): _root_.org.apache.spark.sql.catalyst.expressions.codegen.ExprCode = {

    val (clazz, fres) = SeparateCompilation.withSubExpressions(this, children, outerCtx, ev, ruleSuite.id) {
      (ctx, ruleRunnerExpressionIdx, _) =>

        def hasDefault(when: => String, els: String = ""): String =
          if (ruleSuite.defaultProcessor != NoOpDefaultProcessor.noOp)
            when
          else
            els


        // tester to prove compilation on throughput tests
        // print("I AM GENERATING CODE!!!!")

        // needs resetting every row
        val bufferTerm = ctx.addMutableState(classOf[ArrayBuffer[_]].getName, ctx.freshName("results"))

        // order by salience
        val salience = com.sparkutils.quality.impl.RuleEngineRunnerUtils.flattenSalience(ruleSuite)
        val outputs = 0 until triggerCount
        val reordered = outputs zip salience sortBy(_._2) map(_._1)

        //val outputExprs = children.drop(expressionOffsets.length).map(_.genCode(ctx))

        val arrayData = ctx.freshName("arrayData")
        val z = ctx.freshName("z")
        val o = ctx.freshName("o")

        val addOneS = "org.apache.spark.sql.catalyst.expressions.codegen.QualityCodeGenUtils.addOne"

        // needs addOne as janino can't compile .$plus$eq( and .addOne only exists on 2.13
        def wrapperIf(o: String) =
          if (includeNulls)
            s"$addOneS($bufferTerm, $o);"
          else
            s"""
            if ($o != null) {
              $addOneS($bufferTerm, $o);
            }
            """

        def processFlattenResult(i: Int, outArrTerm: String) =
          if ((canUnroll(i) > 0 && unroll) && isInPlace(i)) {
            // it may have been replaced by subexpr, but it was at one stage an InPlaceArray
            val o = ctx.freshName("o")
            val a = ctx.freshName("a")

            val pre = s"""
              // flatten and canUnroll for InPlaceArray
              Object $o = null;
              Object[] $a = $outArrTerm.array();
            """

            val groupSize = getConfig(UnrollOutputArraySize, s"$unrollOutputArraySize").toInt

            val entries = (0 until canUnroll(i)).grouped(groupSize).toSeq
            val hasLastToDrop =
              entries.lastOption.exists { l =>
                if (l.size == groupSize)
                  false
                else
                  true
              }
            val ofSize =
              if (hasLastToDrop)
                entries.dropRight(1)
              else
                entries

            val lastChunks =
              if (hasLastToDrop)
                entries.last
              else
                Seq.empty

            val loopChunk =
              (0 until groupSize).foldLeft("") {
                (cur, i) =>

                  s"""
                    $cur
                    $o = $a[($z * $groupSize) + $i];
                    ${wrapperIf(o)}
                   """
              }

            val lastChunk =
              lastChunks.indices.foldLeft("") {
                (cur, i) =>

                  s"""
                  $cur

                  $o = $a[${ofSize.size * groupSize} + $i];
                  ${wrapperIf(o)}
                """
              }

            val loop =
              if (ofSize.nonEmpty)
                s"""
                  for (int $z = 0; $z < ${ofSize.size}; $z++) {
                    $loopChunk
                  }
                """
              else
                ""

            val out =
              s"""
                $pre
                $loop
                $lastChunk
                 """
            out

          } else {
            if (isInPlace(i)) { // it may have been replaced but it was at one stage an InPlaceArray
              val o = ctx.freshName("o")
              val a = ctx.freshName("a")

              val pre = s"""
                // flatten case and InPlaceArray - no unroll
                Object $o = null;
                Object[] $a = $outArrTerm.array();
              """

              val out =
              s"""
                $pre

                for (int $z = 0; $z < ${canUnroll(i)}; $z++) {
                  $o = $a[$z];
                  ${wrapperIf(o)}
                }
              """

              out
            } else
              if (canFlatten) s"""
                // flatten case and native CreateArray
                ArrayData $arrayData = (ArrayData) $outArrTerm;
                for (int $z = 0; $z < $arrayData.numElements(); $z++) {
                  Object $o = ${CodeGenerator.getValue(arrayData, elementType, z)};
                  ${wrapperIf(o)}
                }
              """
              else ""
          }

        val salienceFromOffsets = flattenSalience(ruleSuite)

        val compilerTerms =
          RuleEngineRunnerUtils.genCompilerTerms[T](this, ruleRunnerExpressionIdx, outerCtx, ctx,
            PassThroughEvalOnly(children), expressionOffsets, children,
            false, variablesPerFunc, variableFuncGroup, false, extraConfig,
            // capture the current
            extraResult = (outArrTerm: String, i: Int) =>
              s"""
                 if (($outArrTerm != null) || $includeNulls) {
                    if (($outArrTerm == null) || ${!(flatten && canFlatten)}) {
                      $addOneS($bufferTerm, $outArrTerm);
                    } else {
                      ${ processFlattenResult(i, outArrTerm) }
                    }
                 }
               """,
            orderOffset = (idx: Int) => reordered(idx),
            // we shouldn't check salience as we are already ordered by it
            salienceCheck = false,
            sizeAdjustment =
              if (ruleSuite.defaultProcessor != NoOpDefaultProcessor.noOp)
                -1 // don't generate the default, there isn't a trigger
              else
                0,
            salience = salienceFromOffsets(_)
          )

        import compilerTerms._
        import parameterInformation._

        val pre = s"""
              $currentSalience = java.lang.Integer.MAX_VALUE;
              $currentOutputIndex = -1;
              $hasAPassTerm = false;
              $bufferTerm = new ${classOf[ArrayBuffer[_]].getName}($starterSize);

              // copy row
              $resultRowCopy

              // group specific subexprs
              ${grouped.subExpressions}
              // group calls
              ${grouped.groupCalls.map { f => s"$f($paramsCall);" }.mkString("\n")}

              ${
                hasDefault(
                  // if we have a default the result type should be DefaultRule
                  s"""
                    if (!$hasAPassTerm) {
                      $resultRow.update(1, ${
                        DefaultRuleInt
                      });
                    }

                  """)
              }
          """


        val resName = ctx.freshName("result")
        val resNull = ctx.freshName("isNull")

        val exp = ExprCode(VariableValue(resName, ev.value.javaType), isNullVariable(resNull))

        val post = s"""

              boolean ${exp.isNull} = false;
          """

        val res =
          exp.copy(code = code"""
            $pre

            ${hasDefault{
            s"""
                if (!$hasAPassTerm) {
                  ${
                    val defP = children.last.genCode(ctx)
                    s"""
                        ${defP.code}

                        //System.out.println("DefaultProcessor result is ${defP.value}" + ${defP.value});

                        if ((${defP.value} == null) || ${!(flatten && canFlatten)}) {
                          $addOneS($bufferTerm, ${defP.value});
                        } else {
                          ${ // -1 for normal last
                            processFlattenResult(canUnroll.length - 1, defP.value)
                            }
                        }
                      """
                  }
                }
            """
            }}

            InternalRow ${exp.value} =
              com.sparkutils.quality.impl.CollectRunnerUtils.compiledEval(
                $resultRow,
                $bufferTerm);

            $post
            """
          )

        GenerateResult(compilerTerms, res, grouped.extraClasses, grouped.ignoreTopLevelSubExpressions)
    }
    setClazzSource(clazz)
    fres
  }
}

/**
 * Children will be rewritten by the plan, it's then re-incorporated into ruleSuite
 * expressionOffsets.length is the length of the trigger expressions in realChildren, realChildren(expressionOffsets.length + expressionOffsets(x)) will be the correct OutputExpression
 */
case class CollectRunnerRunner(ruleSuite: RuleSuite, children: Seq[Expression], resultDataType: Option[DataType],
                                variablesPerFunc: Int, variableFuncGroup: Int, expressionOffsets: Array[Int],
                               triggerCount: Int, flatten: Boolean, includeNulls: Boolean, canUnroll: Array[Int],
                               isInPlace: Array[Boolean], unroll: Boolean, unrollOutputArraySize: Int,
                               extraConfig: Map[String, String], audited: Boolean = false, alreadyZero: Boolean = false
                               ) extends CollectRunnerBase[CollectRunnerRunner] {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    val r = copy(children = newChildren, audited = true)
    if (!audited) {
      r.performGroupingAuditDump()
    }
    r
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = doGenCodeI(ctx, ev)

  override implicit val classTagT: ClassTag[CollectRunnerRunner] = ClassTag(classOf[CollectRunnerRunner])
  override val tClass: Class[CollectRunnerRunner] = classOf[CollectRunnerRunner]

  override def withZeroCode(): Runner = copy(alreadyZero = true)
}
