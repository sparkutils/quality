package com.sparkutils.quality.impl

import com.sparkutils.quality.impl.MapUtils.getMapEntry
import com.sparkutils.quality.ruleSetType
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.Expression
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
  Expression with CodegenFallback with InputTypeChecks {
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
  protected var cachedRulesPositions: Seq[Int] = Seq.empty

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
          val (result, newCachedR) = RuleResultExpression.getRule(row.getMap(1), cachedRulesPositions, input4.asInstanceOf[Long])
          cachedSetPositions = newCachedR
          result
        }
      } else
        null
    }
  }

  override def dataType: DataType = IntegerType

  override def inputDataTypes: Seq[Seq[DataType]] = Seq(
    Seq(com.sparkutils.quality.ruleSuiteResultType, com.sparkutils.quality.ruleSuiteDetailsResultType),
    Seq(LongType), Seq(LongType), Seq(LongType))
}
