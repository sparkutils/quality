package com.sparkutils.quality.impl

import com.sparkutils.quality.impl.MapUtils.getMapEntry
import com.sparkutils.quality.ruleSetType
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.qualityFunctions.InputTypeChecks
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType}

object RuleResultExpression {

  def getEntry(mapData: MapData, cachedPositions: Seq[Int], id: Long, dataType: DataType): (Any, Seq[Int]) =
    getMapEntry(mapData, cachedPositions, id) {
      (i: Int) => mapData.valueArray().get(i, dataType)
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
        val (row, newCachedS) = RuleResultExpression.getEntry(theStruct.getMap(extractResults), cachedSetPositions, input3.asInstanceOf[Long], entryType)
        cachedSetPositions = newCachedS
        if (row == null)
          null
        else {
          val (result, newCachedR) = RuleResultExpression.getEntry(access(row), cachedRulePositions, input4.asInstanceOf[Long], dataType)
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

    val className = classOf[RuleResultExpression].getName
    val dataTypeName = classOf[DataType].getName

    val referencesIndex = ctx.references.size - 1

    // pass the type through
    val dataTypeRef = ctx.addMutableState(dataTypeName, "dataTypeRef",
      v => s"$v = ($dataTypeName)((($className)references" +
        s"[$referencesIndex]).dataType());")
    val entryTypeRef = ctx.addMutableState(dataTypeName, "entryTypeRef",
      v => s"$v = ($dataTypeName)((($className)references" +
        s"[$referencesIndex]).getEntryType());")

    val structCode = children(0).genCode(ctx)
    val suiteCode = children(1).genCode(ctx)
    val setCode = children(2).genCode(ctx)
    val ruleCode = children(3).genCode(ctx)

    val companion = "com.sparkutils.quality.impl.RuleResultExpression"

    val ruleSetResultClassName = "scala.Tuple2<InternalRow, scala.collection.Seq<Object>>"
    val ruleResultClassName = "scala.Tuple2<Integer, scala.collection.Seq<Object>>"

    val dataTypeJava = CodeGenerator.boxedType(dataType)

    ev.copy(code =
      code"""
         ${structCode.code}
         ${suiteCode.code}
         ${setCode.code}
         ${ruleCode.code}
         boolean ${ev.isNull} = false;
         $dataTypeJava ${ev.value} = ${ CodeGenerator.defaultValue(dataType) };

         if (${structCode.isNull} || ${structCode.isNull} || ${structCode.isNull} || ${structCode.isNull}) {
           ${ev.isNull} = true;
         } else {
           ${classOf[InternalRow].getName} theStruct = ${structCode.value};
           Long suite = theStruct.getLong(0);
           if (suite == ${suiteCode.value}) {
              $ruleSetResultClassName ruleSetResult = $companion.getEntry(theStruct.getMap($extractResults), $cachedSetPositions, ${setCode.value}, $entryTypeRef);
              $cachedSetPositions = (scala.collection.Seq<Object>) ruleSetResult._2();
              if (ruleSetResult._1() == null) {
                ${ev.isNull} = true;
              } else {
                $ruleResultClassName ruleResult = $companion.getEntry((($className)references[$referencesIndex]).access(ruleSetResult._1()), $cachedRulePositions, ${ruleCode.value}, $dataTypeRef);
                $cachedRulePositions = (scala.collection.Seq<Object>) ruleResult._2();
                if (ruleResult._1() == null) {
                  ${ev.isNull} = true;
                } else {
                  ${ev.isNull} = false;
                  ${ev.value} = ($dataTypeJava) ruleResult._1();
                }
              }
            } else {
              ${ev.isNull} = true;
            }
         }
            """)
  }

  lazy val (resultType, entryType, accessF) =
    children(0).dataType match {
      case com.sparkutils.quality.expressionsResultsType =>
        (com.sparkutils.quality.expressionResultType, com.sparkutils.quality.expressionsRuleSetType, (a: Any) => a.asInstanceOf[MapData] )
      case com.sparkutils.quality.expressionsResultsNoDDLType =>
        (StringType, com.sparkutils.quality.expressionsRuleSetNoDDLType, (a: Any) => a.asInstanceOf[MapData])
      case _ =>
        (IntegerType, com.sparkutils.quality.ruleSetType, (a: Any) => a.asInstanceOf[InternalRow].getMap (1) )
    }

  def getEntryType = entryType

  def access(any: Any): MapData = accessF(any)

  override def dataType: DataType = resultType

  override def inputDataTypes: Seq[Seq[DataType]] = Seq(
    Seq(com.sparkutils.quality.ruleSuiteResultType, com.sparkutils.quality.ruleSuiteDetailsResultType,
      com.sparkutils.quality.expressionsResultsType, com.sparkutils.quality.expressionsResultsNoDDLType),
    Seq(LongType), Seq(LongType), Seq(LongType))
}
