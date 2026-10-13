package com.sparkutils.quality.impl.util

import com.sparkutils.shim.expressions.{CreateNamedStruct1, GetStructField3}
import frameless.TypedEncoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{Encoder, ShimUtils}
import org.apache.spark.sql.catalyst.analysis.{GetColumnByOrdinal, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, JavaCode}
import org.apache.spark.sql.catalyst.expressions.{Alias, BoundReference, Expression, If, IsNull, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.objects.{InitializeJavaBean, Invoke, MapObjects, NewInstance, UnresolvedMapObjects}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.language.higherKinds


/**
 * Frameless sets path in foldable encoders to nullable == false, but it really is nullable
 * Spark then just accesses the struct which is null.  This forces codegen only
 */
case class ForceNullable(child: Expression) extends Expression {

  val children = Seq(child)

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = child.eval(input)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val c = child.genCode(ctx)
    val typ = JavaCode.javaType(dataType)
    val boxed = JavaCode.boxedType(dataType)
    ev.copy(code =
      code"""
            ${c.code}
            boolean ${ev.isNull} = true;
            $typ ${ev.value} = null;
            if (${c.value} != null) {
              ${ev.isNull} = false;
              ${ev.value} = ($boxed) ${c.value};
            }
            """)
  }


  override def dataType: DataType = child.dataType

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(child = newChildren.head)
}

