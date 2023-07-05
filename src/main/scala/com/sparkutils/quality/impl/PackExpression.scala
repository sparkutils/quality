package com.sparkutils.quality.impl

import com.sparkutils.quality.Id
import com.sparkutils.quality.impl.imports.RuleResultsImports.packId
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.qualityFunctions.InputTypeChecks
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StructField, StructType}

object Pack {
  def apply(id: Column, version: Column): Column =
    new Column( apply(id.expr, version.expr) )

  def apply(id: Expression, version: Expression) =
    PackExpression( id, version )
}

@ExpressionDescription(
  usage = "packInts(expr, expr) - Returns a long with two packed ints.",
  examples = """
    Examples:
      > SELECT packInts(1, 2);
       10231L
  """)
case class PackExpression(left: Expression, right: Expression) extends BinaryExpression
  with NullIntolerant with InputTypeChecks {

  override def nullSafeEval(id: Any, version: Any): Any = {
    packId(Id(id.asInstanceOf[Int], version.asInstanceOf[Int]))
  }
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (l, r) => s"(Long)com.sparkutils.quality.impl.PackId.packId(new com.sparkutils.quality.Id((Integer)($l), (Integer)($r)))")

  override def dataType: DataType = LongType

  override def inputDataTypes: Seq[Seq[DataType]] = Seq(Seq(IntegerType), Seq(IntegerType))

  protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

object UnPack {
  def apply(packed: Column): Column =
    new Column( apply(packed.expr) )

  def apply( packed: Expression ) =
    UnPackExpression( packed )

  def toRow( id: Id ) = InternalRow(id.id, id.version)
}

@ExpressionDescription(
  usage = "unpack(expr) - Returns a struct with two ints. id and version",
  examples = """
    Examples:
      > SELECT packInts(1, 2);
       10231L
  """)
case class UnPackExpression(child: Expression) extends UnaryExpression
  with NullIntolerant with InputTypeChecks {

  override def nullSafeEval(packed: Any): Any = {
    val id = PackId.unpack(packed.asInstanceOf[Long])
    UnPack.toRow(id)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (c) => s"com.sparkutils.quality.impl.UnPack.toRow( com.sparkutils.quality.impl.PackId.unpack((Long)$c) )")

  override def dataType: DataType = StructType( Seq(
    StructField(name = "id", dataType = IntegerType),
    StructField(name = "version", dataType = IntegerType)
  ))

  override def inputDataTypes: Seq[Seq[DataType]] = Seq(Seq(LongType))

  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}

object UnPackIdTriple {
  def apply(packed: Column): Column =
    new Column( apply(packed.expr) )

  def apply( packed: Expression ) =
    UnPackIdTripleExpression( packed )

  def toRow( packed: Any ): InternalRow =
    if (packed == null)
      packed.asInstanceOf[InternalRow]
    else {
      val i = packed.asInstanceOf[InternalRow]

      val rsuid = PackId.unpack(i.getLong(0))
      val rsid = PackId.unpack(i.getLong(1))
      val ruid = PackId.unpack(i.getLong(2))

      InternalRow(rsuid.id, rsuid.version, rsid.id, rsid.version, ruid.id, ruid.version)
    }
}

@ExpressionDescription(
  usage = "unpackLong(expr) - Returns a struct with two ints. id and version",
  examples = """
    Examples:
      > SELECT packInts(1, 2);
       10231L
  """)
case class UnPackIdTripleExpression(child: Expression) extends UnaryExpression with InputTypeChecks {

  override def nullSafeEval(packed: Any): Any =
    UnPackIdTriple.toRow(packed)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (c) => s"com.sparkutils.quality.impl.UnPackIdTriple.toRow( $c )")

  override def nullable: Boolean = true

  override def dataType: DataType = StructType( Seq(
    StructField(name = "ruleSuiteId", dataType = IntegerType),
    StructField(name = "ruleSuiteVersion", dataType = IntegerType),
    StructField(name = "ruleSetId", dataType = IntegerType),
    StructField(name = "ruleSetVersion", dataType = IntegerType),
    StructField(name = "ruleId", dataType = IntegerType),
    StructField(name = "ruleVersion", dataType = IntegerType)
  ))

  override def inputDataTypes: Seq[Seq[DataType]] = Seq(Seq(com.sparkutils.quality.types.fullRuleIdType))

  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}