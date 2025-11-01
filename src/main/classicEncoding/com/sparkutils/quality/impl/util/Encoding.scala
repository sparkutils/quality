package com.sparkutils.quality.impl.util

import com.sparkutils.shim.expressions.{CreateNamedStruct1, GetStructField3}
import frameless.TypedEncoder
import org.apache.spark.sql.{Encoder, ShimUtils}
import org.apache.spark.sql.catalyst.analysis.{GetColumnByOrdinal, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{Alias, BoundReference, Expression, If, IsNull, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.objects.{InitializeJavaBean, Invoke, MapObjects, NewInstance, UnresolvedMapObjects}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

object Encoding {

  /**
   * Wraps a non Frameless encoder in a TypedEncoder, adjusting paths as needed.
   *
   * This is not intended for general use and is used by the ProcessFunctions.
   *
   * @param outputType
   * @tparam T
   * @return
   */
  def fromNormalEncoder[T: Encoder]: TypedEncoder[T] = {
    val oexpr = ShimUtils.expressionEncoder(implicitly[Encoder[T]])

    implicit val cltag = oexpr.clsTag

    new TypedEncoder[T] {

      override def nullable: Boolean = true

      override def jvmRepr: DataType = oexpr.deserializer.dataType

      override def catalystRepr: DataType = {
        val se = oexpr.serializer
        if (se.length == 1)
          se.head.dataType
        else
          StructType( // 2.4 only cast
            se.map(n => StructField(n.asInstanceOf[NamedExpression].qualifiedName, n.dataType, n.nullable))
          )
      }

      override def fromCatalyst(path: Expression): Expression = {
        val de = oexpr.deserializer
        val r =
          de match {
            case a: Alias =>
              a.child match {
                case m: UnresolvedMapObjects => a.withNewChildren(Seq( m.copy(child = path) ))
                case a => a.transformUp {
                  case _: GetColumnByOrdinal =>
                    path
                }
              }
            case m: UnresolvedMapObjects => m.copy(child = path)
            case n: NewInstance =>
              If(IsNull(ForceNullable(path)), Literal(null), n)
            case i: InitializeJavaBean =>
              If(IsNull(ForceNullable(path)), Literal(null), i)
            // all single fields from a struct
            case i: Invoke =>
              i.transformUp {
                case _: GetColumnByOrdinal =>
                  path
              }
            case a => a.transformUp {
              case _: GetColumnByOrdinal =>
                path
            }
          }
        r

      }

      // only used by resolveAndBind
      override def toCatalyst(path: Expression): Expression = {
        val se = oexpr.serializer
        if (se.length == 1)
          se.head match {
            case a: Alias =>
              a.child match {
                case m: MapObjects => a.withNewChildren(Seq( m.copy(inputData = path) ))
                case a => a.transformUp {
                  case b: BoundReference => path
                }
              }
            case m: MapObjects => m.copy(inputData = path)
            case a => a.transformUp {
              case b: BoundReference => path
            }
          }
        else {
          val o = implicitly[Encoder[T]].schema

          val dealiased = se.map {
            case a: Alias =>
              a.name -> a.child.transformUp {
                case b: BoundReference => path
              }
          }.toMap

          CreateNamedStruct1(
            o.fields.map(f => f.name -> dealiased(f.name)).flatMap {
              case (name, e) =>
                Seq[Expression](Literal(name), e)
            }
          )
        }
      }
    }

  }

}

