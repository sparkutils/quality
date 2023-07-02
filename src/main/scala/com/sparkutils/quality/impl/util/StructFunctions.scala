package com.sparkutils.quality.impl.util

import org.apache.spark.sql.Column
import org.apache.spark.sql.QualitySparkUtils.{toSQLExpr, toSQLType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{Resolver, TypeCheckResult, UnresolvedAttribute, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, Expression, ExtractValue, GetStructField, If, IsNull, LeafExpression, Literal, UnaryExpression, Unevaluable}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

trait StructFunctionsImport {

  /**
   * Adds fields, in order, for each field path it's paired transformation is applied to the update column
   *
   * @param update
   * @param transformations
   * @return a new copy of update with the changes applied
   */
  def update_field(update: Column, transformations: (String, Column)*): Column =
    new Column(
      transformFields{
        transformations.foldRight(update.expr) {
          case ((path, col), origin) =>
            UpdateFields.apply(origin, path, col.expr)
        }
      }
    )

  protected def transformFields(exp: Expression): Expression =
    exp.transform { // simplify, normally done in optimizer UpdateFields
      case UpdateFields(UpdateFields(struct +: fieldOps1) +: fieldOps2) =>
        UpdateFields(struct +: ( fieldOps1 ++ fieldOps2) )
    }

  /**
   * Drops a field from a structure
   * @param update
   * @param fieldNames may be nested
   * @return
   */
  def drop_field(update: Column, fieldNames: String*): Column =
    new Column(
      transformFields{
        fieldNames.foldRight(update.expr) {
          case (fieldName, origin) =>
            UpdateFields.apply(origin, fieldName)
        }
      }
    )
}

// Below is lifted from 3.4.1 complexTypeCreator

/**
 * Represents an operation to be applied to the fields of a struct.
 */
trait StructFieldsOperation extends Expression {

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

  override def foldable: Boolean = false

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

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = ???
}

/**
 * Drop a field by name.
 */
case class DropField(name: String) extends LeafExpression with StructFieldsOperation {
  override def apply(values: Seq[(StructField, Expression)]): Seq[(StructField, Expression)] =
    values.filterNot { case (field, _) => resolver(field.name, name) }

  override def eval(input: InternalRow): Any = ???

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = ???
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