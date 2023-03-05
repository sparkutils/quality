package com.sparkutils.quality.impl.longPair

import com.sparkutils.quality.BloomFilterMap
import com.sparkutils.quality.BloomLookup
import com.sparkutils.quality.impl.bloom.{BloomExpressionLookup, BloomFilterLookup}
import com.sparkutils.quality.impl.id.{GenericLongBasedID, model}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{Column, InputTypeChecks, QualitySparkUtils, SparkSession}

import scala.annotation.tailrec

object LongPair {
  val structType = StructType(Seq(
    StructField("lower", LongType, false),
    StructField("higher", LongType, false)))
}

trait RowIDExpressionImports {
  def rowIDExpression(rng: Column, bloomLookupId: Column, bloomFilterMap: BloomFilterMap): Column =
    new Column(SaferLongPairsExpression(rng.expr, bloomLookupId.expr, bloomFilterMap))

  def registerLongPairFunction(bloomFilterMap: BloomFilterMap) {
    val funcReg = SparkSession.getActiveSession.get.sessionState.functionRegistry

    val f = (exps: Seq[Expression]) => SaferLongPairsExpression(exps(0), exps(1), bloomFilterMap)
    QualitySparkUtils.registerFunction(funcReg)("saferLongPair", f)
  }
}

/**
 * Creates longPairs using an rng and a bloom to lookup generated ids
 * @param left rng
 * @param right lookup id
 * @param bloomMap lookup bloom filter
 */
@deprecated(since = "0.0.1",message = "Use uniqueId instead for row id's, checking existing rng is still a valid case so it is not removed.")
case class SaferLongPairsExpression(left: Expression, right: Expression, bloomMap: BloomFilterMap) extends BinaryExpression with NullIntolerant with CodegenFallback {

  lazy val converter = BloomExpressionLookup.bloomLookupValueConverter(left)

  override def eval(input: InternalRow): Any = {
    val value2 = right.eval(input)
    if (value2 == null) {
      null
    } else {
      val (bloom: BloomLookup, fpp) = bloomMap.getOrElse(value2.toString, BloomFilterLookup.bloomDoesNotExist(value2.toString))

      getRes(bloom, input)
    }
  }

  /**
   * Repeatedly calls the rng and tests for bloom membership, needs the bloom and the rng to be statistically useful
   */
  @tailrec
  private def getRes(bloom: BloomLookup, input: InternalRow): Any = {
    val origRNG = left.eval(input)
    val converted = converter(origRNG)

    val res = bloom.mightContain( converted )
    if (!res)
      origRNG
    else
      getRes(bloom, input)
  }

  override def dataType: DataType = left.dataType

  override def sql: String = s"(saferLongPair(${left.sql}, ${right.sql}))"

  /*  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
      nullSafeCodeGen(ctx, ev, (left, right) => {
        ctx.references += this

        val bfmClassName = classOf[BloomFilterMap].getName
        val bfeClassName = "com.sparkutils.quality.spark.bloom.BloomFilterLookupExpression"
        val bfeExpressionIdx = ctx.references.size - 1
        val bloomMapTerm = ctx.addMutableState(bfmClassName, ctx.freshName("bloomMap"),
          v => s"$v = ($bfmClassName)((($bfeClassName)references" +
            s"[$bfeExpressionIdx]).bloomMap());")

        val bloomPair = ctx.freshName("bloomPair")
        s"""
            scala.Tuple2 $bloomPair = (scala.Tuple2) $bloomMapTerm.getOrElse($right.toString(), com.sparkutils.quality.spark.bloom.BloomFilterLookup.bloomDoesNotExistJ($right.toString()));

            boolean res = ((org.apache.spark.util.sketch.BloomFilter) $bloomPair._1()).mightContain($left);
            if (!res) {
              ${ev.value} = 0.0;
            } else {
              ${ev.value} = (Double) $bloomPair._2();
            }
           """}) */
  protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

object LongPairExpression {
  def genRow(input1: Any, input2: Any) = InternalRow(input1, input2)
}

/**
 * Creates RowIDs using two long fields
 * @param left lower long
 * @param right higher long
 */
case class LongPairExpression(left: Expression, right: Expression) extends BinaryExpression with NullIntolerant
  with InputTypeChecks {

  override protected def nullSafeEval(input1: Any, input2: Any): Any = {
    LongPairExpression.genRow(input1, input2)
  }

  override def dataType: DataType = LongPair.structType

  override def sql: String = s"(longPair(${left.sql}, ${right.sql}))"

  override def inputDataTypes: Seq[Seq[DataType]] = Seq(Seq(LongType), Seq(LongType))

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (input1, input2) =>
      s"com.sparkutils.quality.impl.longPair.LongPairExpression.genRow($input1, $input2)")

  protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

/**
 * Takes a prefixed lower and upper long pair field, must be 128bit with the provided prefix and converts to lower and higher for other functions
 * @param prefix prefix of the nested fields
 * @param child the id field
 */
case class PrefixedToLongPair(child: Expression, prefix: String) extends UnaryExpression with NullIntolerant
  with InputTypeChecks {

  override protected def nullSafeEval(input1: Any): Any = {
    val i = input1.asInstanceOf[InternalRow]
    InternalRow(i.getLong(1), i.getLong(2))
  }

  override def dataType: DataType = LongPair.structType

  override def sql: String = s"(prefixedToLongPair(${child.sql}))"

  override def inputDataTypes: Seq[Seq[DataType]] = Seq(Seq(GenericLongBasedID(model.ProvidedID, Array.ofDim[Long](2)).dataType(prefix)))

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (input1) =>
      s"new GenericInternalRow(new Object[]{((InternalRow)$input1).getLong(1), ((InternalRow)$input1).getLong(2)})")

  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}