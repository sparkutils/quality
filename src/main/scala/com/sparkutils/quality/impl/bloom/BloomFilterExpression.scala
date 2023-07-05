package com.sparkutils.quality.impl.bloom

import com.sparkutils.quality.QualityException.qualityException
import com.sparkutils.quality.{BloomFilterMap, RuleSuite}
import com.sparkutils.quality.impl.{RuleRegistrationFunctions, RuleRunnerUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription, Literal, NullIntolerant}
import org.apache.spark.sql.types.{DataType, DoubleType, StringType}
import org.apache.spark.sql.{Column, QualitySparkUtils}

object BloomFilterLookup {

  /**
   * Identifies bloom ids before (or after) resolving for a given ruleSuite, use to know which bloom filters need to be loaded
   *
   * @param ruleSuite a ruleSuite full of expressions to check
   * @return The bloom id's used, for unresolved expression trees this may contain blooms which are not present in the bloom map
   */
  def getBlooms(ruleSuite: RuleSuite): Seq[String]  = {
    // parsing is different than plan..
    val flattened = RuleRunnerUtils.flattenExpressions(ruleSuite)
    val ids = flattened.flatMap {
      exp =>
        BloomFilterLookup.getBlooms(exp)
    }
    ids
  }

  /**
    * For withColumn / select usage, the bloomfilters generation and test expressions must be of the same type
    */
  def apply(bloomFilterName: Column, lookupValue: Column, bloomMap: Broadcast[BloomFilterMap]): Column =
    new Column(BloomFilterLookupExpression(bloomFilterName.expr, lookupValue.expr, bloomMap))

  private[impl] def bloomDoesNotExist(bloom: String) = qualityException("The bloom filter: "+bloom+", does not exist in the provided bloomMap")

  def bloomDoesNotExistJ(bloom: String) = () => bloomDoesNotExist(bloom)

  /**
   * Identifies blooms from either a resolved or unresolved expression
   *
   * @param expression a single Expression, can be a complex tree or simple direct probabilityIn or rowid
   * @return The bloom id's used, for unresolved expression trees this may contain blooms which are not present in the bloom map
   */
  def getBlooms(expression: Expression): Seq[String] = BloomFilterLookupSparkVersionSpecific.getBlooms(expression)

}

/**
  * Returns a 0.0 for not present or 1 - FPP value for probably present (i.e. error rate of 0.01 will return 0.99 percent likely throws exception if the table is not present
  * @param right the name of the map entry / dataframe the bloomfilter belongs to
  * @param left the expression to derive the hash - this must match the same expression used to fill the bloomfilter
  * @param bloomMap
  *
  */
@ExpressionDescription(
  usage = "_FUNC_(content to lookup, bloomFilterName) - Returns either 0.0 for not present, or 1 - fpp from the bloomfilter",
  examples = """
    Examples:
      > SELECT _FUNC_('a thing that might be there', 'otherDataset');
       0.9
  """,
  since = "1.5.0")
case class BloomFilterLookupExpression(left: Expression, right: Expression, bloomMap: Broadcast[BloomFilterMap]) extends BinaryExpression with NullIntolerant {//with CodegenFallback {

  val map = bloomMap.value
  lazy val bloom = map.getOrElse(right.eval().toString, BloomFilterLookup.bloomDoesNotExist(right.eval().toString))
  @inline final def bloomF(right: Any) = right match {
    case Literal(a, StringType) => bloom
    case _ => map.getOrElse(right.toString, BloomFilterLookup.bloomDoesNotExist(right.toString))
  }

  lazy val converter = BloomExpressionLookup.bloomLookupValueConverter(left)
  lazy val isPrimitive = QualitySparkUtils.isPrimitive(left.dataType) // in which case it's identity

  override def nullSafeEval(left: Any, right: Any): Any = {
    val (bloom: com.sparkutils.quality.BloomLookup, fpp) = bloomF(right)

    val converted =
      if (isPrimitive)
        left
      else
        converter(left)

    val res = bloom.mightContain( converted )
    if (!res)
      0.0
    else
      fpp
  }

  override def dataType: DataType = DoubleType

  override def sql: String = s"(probabilityIn(${left.sql}, ${right.sql}))"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    ctx.references += this

    val bfmClassName = classOf[BloomFilterMap].getName
    val bClassName = classOf[Broadcast[BloomFilterMap]].getName
    val bfeClassName = "com.sparkutils.quality.impl.bloom.BloomFilterLookupExpression"
    val bfeExpressionIdx = ctx.references.size - 1
    val bloomMapTerm = ctx.addMutableState(bClassName, ctx.freshName("bloomMap"),
      v => s"$v = ($bClassName) ((($bfeClassName)references" +
        s"[$bfeExpressionIdx]).bloomMap());")

    val cClassName = classOf[Any => Any].getName
    val converterTerm = ctx.addMutableState(cClassName, ctx.freshName("converter"),
      v => s"$v = ($cClassName) (com.sparkutils.quality.impl.bloom.BloomExpressionLookup.bloomLookupValueConverter( ((($bfeClassName)references" +
        s"[$bfeExpressionIdx]).left()) ));")

    val blClassName = "scala.Tuple2"
    val bloomTerm = ctx.addMutableState(blClassName, ctx.freshName("bloom"),
      v => s"$v = ($blClassName) ((($bfeClassName)references" +
        s"[$bfeExpressionIdx]).bloom());")

    val lefteval = left.genCode(ctx)
    val leftCode =
      s"""${lefteval.code}\n
          """

    val converted =
      if (isPrimitive)
        lefteval.value
      else
        s"$converterTerm.apply(${lefteval.value})"

    val (bloomPairSetter, rightCode) =
      right match {
        case Literal(a, StringType) => (bloomTerm, "")
        case _ =>
          val righteval = right.genCode(ctx)

          (s"""(scala.Tuple2) (($bfmClassName)$bloomMapTerm.value()).
              |          getOrElse(${righteval.value}.toString(), com.sparkutils.quality.impl.bloom.BloomFilterLookup.bloomDoesNotExistJ(${righteval.value}.toString()))""".stripMargin,
          s"""${righteval.code}\n
          """)
      }

    val bloomPair = ctx.freshName("bloomPair")
    ev.copy(code = code"""

        $leftCode
        $rightCode

        scala.Tuple2 $bloomPair = $bloomPairSetter;

        Object converted = $converted;

        boolean res = ((com.sparkutils.quality.BloomLookup) $bloomPair._1()).mightContain(converted);
        double ${ev.value} = (!res) ? 0.0 : (Double) $bloomPair._2();
        boolean ${ev.isNull} = false;
       """)
  }

  protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}
