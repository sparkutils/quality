package com.sparkutils.quality.impl.util

import org.apache.spark.sql.Column
import org.apache.spark.sql.QualitySparkUtils.{toSQLExpr, toSQLType}
import org.apache.spark.sql.catalyst.analysis.{Resolver, UnresolvedAttribute, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, Expression, ExpressionDescription, ExtractValue, GenericInternalRow, GetStructField, If, IsNull, LeafExpression, Literal, UnaryExpression, Unevaluable}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

import scala.collection.mutable.ArrayBuffer

trait StructFunctionsImport {

  /**
   * Adds fields, in order, for each field path it's paired transformation is applied to the update column
   *
   * @param update
   * @param transformations
   * @return a new copy of update with the changes applied
   */ // TODO figure out optimisation to join fields
  def update_field(update: Column, transformations: (String, Column)*): Column =
/*    new Column( AddFields(update.expr +: transformations.flatMap{ transformation
        => Seq(lit(transformation._1).expr, transformation._2.expr)} ) )
*/
    transformations.foldRight(update){
      case ((path, col), origin) =>
        new Column( UpdateFields.apply(origin.expr, path, col.expr) )
    }

}

/**
 * The simple UpdateFields(exps(0), RuleRunnerFunctions.getString(exps(1)), exps(2)) does not work on 3.1.1
 * As such the sme approach must be taken.
 */

// below c+p'd from https://raw.githubusercontent.com/fqaiser94/mse/master/src/main/scala/org/apache/spark/sql/catalyst/expressions/AddFields.scala , apache 2 but only for 2.4
// only withNewChildrenInternal is added for 3.2 support and fixed nameparts

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Adds/replaces fields in a struct.
 * Returns null if struct is null.
 * If multiple fields already exist with the one of the given fieldNames, they will all be replaced.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(struct, name1, val1, name2, val2, ...) - Adds/replaces fields in struct by name.",
  examples = """
    Examples:
      > SELECT _FUNC_({"a":1}, "b", 2, "c", 3);
       {"a":1,"b":2,"c":3}
  """)
// scalastyle:on line.size.limit
case class AddFields(children: Seq[Expression]) extends Expression {

  private lazy val struct: Expression = children.head
  private lazy val (nameExprs, valExprs) = children.drop(1).grouped(2).map {
    case Seq(name, value) => (name, value)
  }.toList.unzip
  private lazy val fieldNames = nameExprs.map(_.eval().asInstanceOf[UTF8String].toString)
  private lazy val pairs = fieldNames.zip(valExprs)

  override def nullable: Boolean = struct.nullable

  private lazy val ogStructType: StructType =
    struct.dataType.asInstanceOf[StructType]

  override lazy val dataType: StructType = {
    val existingFields = ogStructType.fields.map { x => (x.name, x) }
    val addOrReplaceFields = pairs.map { case (fieldName, field) =>
      (fieldName, StructField(fieldName, field.dataType, field.nullable))
    }
    val newFields = loop(existingFields, addOrReplaceFields).map(_._2)
    StructType(newFields)
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.size % 2 == 0) {
      return TypeCheckResult.TypeCheckFailure(s"$prettyName expects an odd number of arguments.")
    }

    val typeName = struct.dataType.typeName
    val expectedStructType = StructType(Nil).typeName
    if (typeName != expectedStructType) {
      return TypeCheckResult.TypeCheckFailure(
        s"Only $expectedStructType is allowed to appear at first position, got: $typeName.")
    }

    if (nameExprs.contains(null) || nameExprs.exists(e => !(e.foldable && e.dataType == StringType))) {
      return TypeCheckResult.TypeCheckFailure(
        s"Only non-null foldable ${StringType.catalogString} expressions are allowed to appear at even position.")
    }

    if (valExprs.contains(null)) {
      return TypeCheckResult.TypeCheckFailure(
        s"Only non-null expressions are allowed to appear at odd positions after first position.")
    }

    TypeCheckResult.TypeCheckSuccess
  }

  override def eval(input: InternalRow): Any = {
    val structValue = struct.eval(input)
    if (structValue == null) {
      null
    } else {
      val existingValues: Seq[(FieldName, Any)] =
        ogStructType.fieldNames.zip(structValue.asInstanceOf[InternalRow].toSeq(ogStructType))
      val addOrReplaceValues: Seq[(FieldName, Any)] =
        pairs.map { case (fieldName, expression) => (fieldName, expression.eval(input)) }
      val newValues = loop(existingValues, addOrReplaceValues).map(_._2)
      InternalRow.fromSeq(newValues)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val structGen = struct.genCode(ctx)
    val addOrReplaceFieldsGens = valExprs.map(_.genCode(ctx))
    val resultCode: String = {
      val structVar = structGen.value
      type NullCheck = String
      type NonNullValue = String
      val existingFieldsCode: Seq[(FieldName, (NullCheck, NonNullValue))] =
        ogStructType.fields.zipWithIndex.map {
          case (structField, i) =>
            val nullCheck = s"$structVar.isNullAt($i)"
            val nonNullValue = CodeGenerator.getValue(structVar, structField.dataType, i.toString)
            (structField.name, (nullCheck, nonNullValue))
        }
      val addOrReplaceFieldsCode: Seq[(FieldName, (NullCheck, NonNullValue))] =
        fieldNames.zip(addOrReplaceFieldsGens).map {
          case (fieldName, fieldExprCode) =>
            val nullCheck = fieldExprCode.isNull.code
            val nonNullValue = fieldExprCode.value.code
            (fieldName, (nullCheck, nonNullValue))
        }
      val newFieldsCode = loop(existingFieldsCode, addOrReplaceFieldsCode)
      val rowClass = classOf[GenericInternalRow].getName
      val rowValuesVar = ctx.freshName("rowValues")
      val populateRowValuesVar = newFieldsCode.zipWithIndex.map {
        case ((_, (nullCheck, nonNullValue)), i) =>
          s"""
             |if ($nullCheck) {
             | $rowValuesVar[$i] = null;
             |} else {
             | $rowValuesVar[$i] = $nonNullValue;
             |}""".stripMargin
      }.mkString("\n|")

      s"""
         |Object[] $rowValuesVar = new Object[${dataType.length}];
         |
         |${addOrReplaceFieldsGens.map(_.code).mkString("\n")}
         |$populateRowValuesVar
         |
         |${ev.value} = new $rowClass($rowValuesVar);
          """.stripMargin
    }

    if (nullable) {
      val nullSafeEval =
        structGen.code + ctx.nullSafeExec(struct.nullable, structGen.isNull) {
          s"""
             |${ev.isNull} = false; // resultCode could change nullability.
             |$resultCode
             |""".stripMargin
        }

      ev.copy(code =
        code"""
          boolean ${ev.isNull} = true;
          ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
          $nullSafeEval
          """)
    } else {
      ev.copy(code =
        code"""
          ${structGen.code}
          ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
          $resultCode
          """, isNull = FalseLiteral)
    }
  }

  override def prettyName: String = "add_fields"

  private type FieldName = String

  /**
   * Recursively loop through addOrReplaceFields, adding or replacing fields by FieldName.
   */
  @scala.annotation.tailrec
  private def loop[V](existingFields: Seq[(String, V)],
                      addOrReplaceFields: Seq[(String, V)]): Seq[(String, V)] = {
    if (addOrReplaceFields.nonEmpty) {
      val existingFieldNames = existingFields.map(_._1)
      val newField@(newFieldName, _) = addOrReplaceFields.head

      if (existingFieldNames.contains(newFieldName)) {
        loop(
          existingFields.map {
            case (fieldName, _) if fieldName == newFieldName => newField
            case x => x
          },
          addOrReplaceFields.drop(1))
      } else {
        loop(
          existingFields :+ newField,
          addOrReplaceFields.drop(1))
      }
    } else {
      existingFields
    }
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(children = newChildren)
}

// Below is lifted from 3.4.1 complexTypeCreator

/**
 * Represents an operation to be applied to the fields of a struct.
 */
trait StructFieldsOperation extends Expression with Unevaluable {

  override lazy val foldable = false

  val resolver: Resolver = SQLConf.get.resolver

  override def dataType: DataType = throw new IllegalStateException(
    "StructFieldsOperation.dataType should not be called.")

  override def nullable: Boolean = throw new IllegalStateException(
    "StructFieldsOperation.nullable should not be called.")

  /**
   * Returns an updated list of StructFields and Expressions that will ultimately be used
   * as the fields argument for [[StructType]] and as the children argument for
   * [[CreateNamedStruct]] respectively inside of [[UpdateFields]].
   */
  def apply(values: Seq[(StructField, Expression)]): Seq[(StructField, Expression)]
}

/**
 * Add or replace a field by name.
 *
 * We extend [[Unevaluable]] here to ensure that [[UpdateFields]] can include it as part of its
 * children, and thereby enable the analyzer to resolve and transform valExpr as necessary.
 */
case class WithField(name: String, child: Expression)
  extends UnaryExpression with StructFieldsOperation {

  override def apply(values: Seq[(StructField, Expression)]): Seq[(StructField, Expression)] = {
    val newFieldExpr = (StructField(name, child.dataType, child.nullable), child)
    val result = ArrayBuffer.empty[(StructField, Expression)]
    var hasMatch = false
    for (existingFieldExpr @ (existingField, _) <- values) {
      if (resolver(existingField.name, name)) {
        hasMatch = true
        result += newFieldExpr
      } else {
        result += existingFieldExpr
      }
    }
    if (!hasMatch) result += newFieldExpr
    result.toSeq
  }

  override def prettyName: String = "WithField"

  protected def withNewChildInternal(newChild: Expression): WithField =
    copy(child = newChild)
}

/**
 * Drop a field by name.
 */
case class DropField(name: String) extends LeafExpression with StructFieldsOperation {
  override def apply(values: Seq[(StructField, Expression)]): Seq[(StructField, Expression)] =
    values.filterNot { case (field, _) => resolver(field.name, name) }
}

/**
 * Updates fields in a struct.
 */
case class UpdateFields(children: Seq[Expression])
  extends Expression with CodegenFallback {

  val structExpr = children.head
  val fieldOps: Seq[StructFieldsOperation] = children.drop(1).map(_.asInstanceOf[StructFieldsOperation])

  override def checkInputDataTypes(): TypeCheckResult = {
    val dataType = structExpr.dataType
    if (!dataType.isInstanceOf[StructType]) {
      TypeCheckResult.TypeCheckFailure( message =
        s"UNEXPECTED_INPUT_TYPE, requiredType StructType, inputSql ${ toSQLExpr(structExpr) }, inputType ${ toSQLType(structExpr.dataType) }"
      )
    } else if (newExprs.isEmpty) {
      TypeCheckResult.TypeCheckFailure( message =
        s"errorSubClass = CANNOT_DROP_ALL_FIELDS"
      )
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }
/*
  override def children: Seq[Expression] = structExpr +: fieldOps.collect {
    case e: Expression => e
  } */

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)

  override def dataType: StructType = StructType(newFields)

  override def nullable: Boolean = structExpr.nullable

  override def prettyName: String = "update_fields"

  private lazy val newFieldExprs: Seq[(StructField, Expression)] = {
    def getFieldExpr(i: Int): Expression = structExpr match {
      case c: CreateNamedStruct => c.valExprs(i)
      case _ => GetStructField(structExpr, i)
    }
    val fieldsWithIndex = structExpr.dataType.asInstanceOf[StructType].fields.zipWithIndex
    val existingFieldExprs: Seq[(StructField, Expression)] =
      fieldsWithIndex.map { case (field, i) => (field, getFieldExpr(i)) }
    fieldOps.foldLeft(existingFieldExprs)((exprs, op) => op(exprs))
  }

  private lazy val newFields: Seq[StructField] = newFieldExprs.map(_._1)

  lazy val newExprs: Seq[Expression] = newFieldExprs.map(_._2)

  lazy val evalExpr: Expression = {
    val createNamedStructExpr = CreateNamedStruct(newFieldExprs.flatMap {
      case (field, expr) => Seq(Literal(field.name), expr)
    })

    if (structExpr.nullable) {
      If(IsNull(structExpr), Literal(null, dataType), createNamedStructExpr)
    } else {
      createNamedStructExpr
    }
  }

  override def eval(input: InternalRow): Any = evalExpr.eval(input)

}

object UpdateFields {
  private def nameParts(fieldName: String): Seq[String] = {
    require(fieldName != null, "fieldName cannot be null")

    if (fieldName.isEmpty) {
      fieldName :: Nil
    } else {
      UnresolvedAttribute.parseAttributeName(fieldName)
      //CatalystSqlParser.parseMultipartIdentifier(fieldName)
    }
  }

  /**
   * Adds/replaces field of `StructType` into `col` expression by name.
   */
  def apply(col: Expression, fieldName: String, expr: Expression): UpdateFields =
    updateFieldsHelper(col, nameParts(fieldName), name => WithField(name, expr))

  /**
   * Drops fields of `StructType` in `col` expression by name.
   */
  def apply(col: Expression, fieldName: String): UpdateFields =
    updateFieldsHelper(col, nameParts(fieldName), name => DropField(name))

  private def updateFieldsHelper(
                                  structExpr: Expression,
                                  namePartsRemaining: Seq[String],
                                  valueFunc: String => StructFieldsOperation) : UpdateFields = {
    val fieldName = namePartsRemaining.head
    if (namePartsRemaining.length == 1) {
      UpdateFields(Seq(structExpr, valueFunc(fieldName)))
    } else {
      val newStruct = if (structExpr.resolved) {
        val resolver = SQLConf.get.resolver
        ExtractValue(structExpr, Literal(fieldName), resolver)
      } else {
        UnresolvedExtractValue(structExpr, Literal(fieldName))
      }

      val newValue = updateFieldsHelper(
        structExpr = newStruct,
        namePartsRemaining = namePartsRemaining.tail,
        valueFunc = valueFunc)
      UpdateFields(Seq(structExpr, WithField(fieldName, newValue) ))
    }
  }
}