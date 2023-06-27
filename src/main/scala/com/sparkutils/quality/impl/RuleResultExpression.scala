package com.sparkutils.quality.impl

import com.sparkutils.quality.impl.MapUtils.getMapEntry
import com.sparkutils.quality.ruleSetType
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.qualityFunctions.InputTypeChecks
import org.apache.spark.sql.types.{DataType, IntegerType, LongType}

object RuleResultExpression {

  def getRuleSet(mapData: MapData, cachedPositions: Seq[Int], id: Long): (InternalRow, Seq[Int]) =
    getMapEntry[InternalRow](mapData, cachedPositions, id) {
      (i: Int) => mapData.valueArray().getStruct(i, ruleSetType.size)
    }

  def getRule(mapData: MapData, cachedPositions: Seq[Int], id: Long): (Integer, Seq[Int]) =
    getMapEntry(mapData, cachedPositions, id) {
      (i: Int) => mapData.valueArray().getInt(i)
    }

  def apply(ruleSuiteResultsColumn: Column, ruleSuiteId: Column, ruleSetId: Column, ruleId: Column): RuleResultExpression =
    new RuleResultExpression(Seq(ruleSuiteResultsColumn.expr, ruleSuiteId.expr, ruleSetId.expr, ruleId.expr))

}

//TODO move to Quanternary after 2.4 is dropped
case class RuleResultExpression(children: Seq[Expression]) extends
  Expression with InputTypeChecks {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)

  protected lazy val extractResults =
    if (children(0).dataType == com.sparkutils.quality.ruleSuiteResultType)
      2
    else
      1

  @transient
  protected var cachedSetPositions: Seq[Int] = Seq.empty

  @transient
  protected var cachedRulePositions: Seq[Int] = Seq.empty

  override def nullable: Boolean = true

  override def eval(inputRow: InternalRow): Any = {
    val noneAreNull = Seq(children(0).eval(inputRow), children(1).eval(inputRow),
      children(2).eval(inputRow), children(3).eval(inputRow))
    if (noneAreNull.contains(null))
      null
    else {
      val Seq(input1, input2, input3, input4) = noneAreNull

      val theStruct = input1.asInstanceOf[InternalRow]
      val suite = theStruct.getLong(0)
      if (suite == input2) {
        val (row, newCachedS) = RuleResultExpression.getRuleSet(theStruct.getMap(extractResults), cachedSetPositions, input3.asInstanceOf[Long])
        cachedSetPositions = newCachedS
        if (row eq null)
          null
        else {
          val (result, newCachedR) = RuleResultExpression.getRule(row.getMap(1), cachedRulePositions, input4.asInstanceOf[Long])
          cachedRulePositions = newCachedR
          result
        }
      } else
        null
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {

    ctx.references += this
    val setClass = classOf[Seq[Int]].getName+"<Object>"
    val cachedSetPositions = ctx.addMutableState(setClass, "cachedSetPositions",
      v => s"$v = ($setClass) scala.collection.Seq$$.MODULE$$.<Object>empty();")
    val cachedRulePositions = ctx.addMutableState(setClass, "cachedRulePositions",
      v => s"$v = ($setClass) scala.collection.Seq$$.MODULE$$.<Object>empty();")

    val structCode = children(0).genCode(ctx)
    val suiteCode = children(1).genCode(ctx)
    val setCode = children(2).genCode(ctx)
    val ruleCode = children(3).genCode(ctx)

    val companion = "com.sparkutils.quality.impl.RuleResultExpression"

    val ruleSetResultClassName = "scala.Tuple2<InternalRow, scala.collection.Seq<Object>>"
    val ruleResultClassName = "scala.Tuple2<Integer, scala.collection.Seq<Object>>"

    ev.copy(code =
      code"""
         ${structCode.code}
         ${suiteCode.code}
         ${setCode.code}
         ${ruleCode.code}
         boolean ${ev.isNull} = false;
         int ${ev.value} = 0; // failed by default

         if (${structCode.isNull} || ${structCode.isNull} || ${structCode.isNull} || ${structCode.isNull}) {
           ${ev.isNull} = true;
         } else {
           ${classOf[InternalRow].getName} theStruct = ${structCode.value};
           Long suite = theStruct.getLong(0);
           if (suite == ${suiteCode.value}) {
              $ruleSetResultClassName ruleSetResult = $companion.getRuleSet(theStruct.getMap($extractResults), $cachedSetPositions, ${setCode.value});
              $cachedSetPositions = (scala.collection.Seq<Object>) ruleSetResult._2();
              if (ruleSetResult._1() == null) {
                ${ev.isNull} = true;
              } else {
                $ruleResultClassName ruleResult = $companion.getRule(((${classOf[InternalRow].getName})ruleSetResult._1()).getMap(1), $cachedRulePositions, ${ruleCode.value});
                $cachedRulePositions = (scala.collection.Seq<Object>) ruleResult._2();
                if (ruleResult._1() == null) {
                  ${ev.isNull} = true;
                } else {
                  ${ev.isNull} = false;
                  ${ev.value} = (java.lang.Integer) ruleResult._1();
                }
              }
            } else {
              ${ev.isNull} = true;
            }
         }
            """)
  }

  override def dataType: DataType = IntegerType

  override def inputDataTypes: Seq[Seq[DataType]] = Seq(
    Seq(com.sparkutils.quality.ruleSuiteResultType, com.sparkutils.quality.ruleSuiteDetailsResultType),
    Seq(LongType), Seq(LongType), Seq(LongType))
}
