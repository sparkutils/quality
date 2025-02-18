package com.sparkutils.quality.impl

import com.sparkutils.quality._
import types._
import com.sparkutils.quality.impl.util.Serializing
import org.apache.spark.sql.ShimUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.shim.expressions.InputTypeChecks
import org.apache.spark.sql.types._

object FlattenStruct {

  val dataType: StructType = StructType(Array(
    StructField("ruleSuiteId", IntegerType),
    StructField("ruleSuiteVersion", IntegerType),
    StructField("ruleSuiteResult", IntegerType),
    StructField("ruleSetResult", IntegerType),
    StructField("ruleSetId", IntegerType),
    StructField("ruleSetVersion", IntegerType),
    StructField("ruleId", IntegerType),
    StructField("ruleVersion", IntegerType),
    StructField("ruleResult", IntegerType)
  ))

  /**
   * Creates a deserializer expression from internal row to RuleSuiteResult
   * @return
   */
  def ruleSuiteDeserializer =
    ShimUtils.expressionEncoder(Encoders.ruleSuiteResultExpEnc).resolveAndBind().deserializer
}

// NB actually adding the variables requires Star's, they only exist in plans against names

@ExpressionDescription(
  usage = "flattenResults(expr) - Returns the result from a result type column row as rows.",
  examples = """
    Examples:
      > SELECT explode(DQ);
       1, 2, 4, 5 etc
  """)
case class FlattenResultsExpression(child: Expression, deserializer: Expression) extends UnaryExpression with CodegenFallback with NullIntolerant
  with InputTypeChecks {
  //assert(child.dataType == SparkHelpers.ruleSuiteResultType, "Only RuleSuiteResult type parameters are supported")

  // compile it out
  private val deserializerW = ExpressionWrapper(deserializer)

  override def nullSafeEval(col: Any): Any = {
    val res = deserializerW.internalEval(col.asInstanceOf[InternalRow]).asInstanceOf[RuleSuiteResult]
    val flattened = Serializing.flatten(res)
    new GenericArrayData(flattened.map {
      row =>
        import row._
        InternalRow(
          ruleSuiteId,
          ruleSuiteVersion,
          ruleSuiteResult,
          ruleSetResult,
          ruleSetId,
          ruleSetVersion,
          ruleId,
          ruleVersion,
          ruleResult
        )
    })
  }

  override def nullable: Boolean = false

  override def dataType: DataType = ArrayType(FlattenStruct.dataType)

  override def inputDataTypes: Seq[Seq[DataType]] = Seq(Seq(ruleSuiteResultType))

  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}

@ExpressionDescription(
  usage = "flattenRulesResults(expr) - Returns the result from a result type column row as rows.",
  examples = """
    Examples:
      > SELECT explode(DQ);
       1, 2, 4, 5 etc
  """)
case class FlattenRulesResultsExpression(child: Expression, deserializer: Expression) extends UnaryExpression with CodegenFallback with NullIntolerant {
  //assert(child.dataType == SparkHelpers.ruleSuiteResultType, "Only RuleSuiteResult type parameters are supported")

  lazy val ruleProductType = {
    val struct = child.dataType.asInstanceOf[StructType]
    struct.fields(2).dataType

    //right.dataType
  }

  // compile it out
  private val deserializerW = ExpressionWrapper(deserializer)

  override def eval(input: InternalRow): Any = {
    val field = child.eval(input).asInstanceOf[InternalRow]
    val res = deserializerW.internalEval(field.get(0, ruleSuiteResultType).asInstanceOf[InternalRow]).asInstanceOf[RuleSuiteResult]
    val flattened = Serializing.flatten(res)
    new GenericArrayData(flattened.map {
      row =>
        import row._
        InternalRow(
          ruleSuiteId,
          ruleSuiteVersion,
          ruleSuiteResult,
          ruleSetResult,
          ruleSetId,
          ruleSetVersion,
          ruleId,
          ruleVersion,
          ruleResult, field.get(1, fullRuleIdType), field.get(2, ruleProductType)
        )
    })
  }

  override def nullable: Boolean = false

  override def dataType: DataType = ArrayType(FlattenStruct.dataType.copy(fields = (FlattenStruct.dataType.fields :+
    StructField("salientRule", fullRuleIdType)) :+ StructField("result", ruleProductType) ))

  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}

/**
 * NB this doesn't make much sense for folder unless in debug
 * @param child
 * @param deserializer
 */
@ExpressionDescription(
  usage = "flattenFolderResults(expr) - Returns the result from a result type column row as rows.",
  examples = """
    Examples:
      > SELECT explode(DQ);
       1, 2, 4, 5 etc
  """)
case class FlattenFolderResultsExpression(child: Expression, deserializer: Expression) extends UnaryExpression with CodegenFallback with NullIntolerant {
  //assert(child.dataType == SparkHelpers.ruleSuiteResultType, "Only RuleSuiteResult type parameters are supported")

  lazy val ruleProductType = {
    val struct = child.dataType.asInstanceOf[StructType]
    struct.fields(1).dataType

    //right.dataType
  }

  // compile it out
  private val deserializerW = ExpressionWrapper(deserializer)

  override def eval(input: InternalRow): Any = {
    val field = child.eval(input).asInstanceOf[InternalRow]
    val res = deserializerW.internalEval(field.get(0, ruleSuiteResultType).asInstanceOf[InternalRow]).asInstanceOf[RuleSuiteResult]
    val flattened = Serializing.flatten(res)
    new GenericArrayData(flattened.map {
      row =>
        import row._
        InternalRow(
          ruleSuiteId,
          ruleSuiteVersion,
          ruleSuiteResult,
          ruleSetResult,
          ruleSetId,
          ruleSetVersion,
          ruleId,
          ruleVersion,
          ruleResult, field.get(1, ruleProductType)
        )
    })
  }

  override def nullable: Boolean = false

  override def dataType: DataType = ArrayType(FlattenStruct.dataType.copy(fields = (FlattenStruct.dataType.fields :+
    StructField("result", ruleProductType) )))

  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}