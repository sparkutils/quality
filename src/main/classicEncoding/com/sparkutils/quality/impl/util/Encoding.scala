package com.sparkutils.quality.impl.util

import com.sparkutils.shim.expressions.{CreateNamedStruct1, GetStructField3}
import frameless.TypedEncoder
import org.apache.spark.sql.{Encoder, ShimUtils}
import org.apache.spark.sql.catalyst.analysis.{GetColumnByOrdinal, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{Alias, BoundReference, Expression, If, IsNull, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.objects.{InitializeJavaBean, Invoke, MapObjects, NewInstance, UnresolvedMapObjects}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.language.higherKinds

/**
 * Provides correction needed to types from field order
 * @tparam
 */
trait EmbeddedTypeCorrection {
  def correctDeserializer(expr: Expression, analyzed: LogicalPlan): Expression
}
object EmbeddedTypeCorrection {

  val noCorrection: EmbeddedTypeCorrection = (expr: Expression, analyzed: LogicalPlan) => expr

  def findInPlan(resultName: String, plan: LogicalPlan) =
    plan.find(_.output.exists(_.name == resultName)).
      flatMap(_.output.find(f => f.name == resultName && f.dataType.isInstanceOf[StructType]))

  def of[T: Encoder](topField: String): EmbeddedTypeCorrection =
    (expr: Expression, analyzed: LogicalPlan) => {
      val e = implicitly[Encoder[T]]
      findInPlan(topField, analyzed) match {
        case Some(attr) =>
          attr.dataType match {
            case structType: StructType =>
              val o = structType.zipWithIndex.map { case (e, i) => e.name -> i }.toMap
              expr.transformUp {
                case n: NewInstance =>
                  n.transform {
                    case u: UnresolvedAttribute if o.contains(u.name) =>
                      // println(s"original from encoder ${u.name} index from analyzed type ${o(u.name)} maps to ${targetType(o(u.name))}")
                      UnresolvedAttribute(attr.name + "." + u.name) //+targetType(o(u.name)).name)
                  }
              }
            case _ => expr
          }
        case _ => expr
      }
    }

  def ofRuleEngine[T: Encoder]: EmbeddedTypeCorrection = of("result")

  def ofRuleFolder[T: Encoder]: EmbeddedTypeCorrection = ofRuleEngine

  def ofExpressionResult[T: Encoder]: EmbeddedTypeCorrection = ofRuleEngine

  def ofExpressionResultNoDDL = noCorrection
}

object Encoding {

  /**
   * Wraps a non-Frameless encoder in a TypedEncoder, adjusting paths as needed.
   *
   * This is not intended for general use and is used by the ProcessFunctions.
   *
   * @tparam T
   * @return
   */
  def fromNormalEncoder[T: Encoder]: TypedEncoder[T] = fromNormalEncoderWithType(None)

    /**
   * Wraps a non-Frameless encoder in a TypedEncoder, adjusting paths as needed.
   *
   * This is not intended for general use and is used by the ProcessFunctions.
   *
   * @tparam T
   * @return
   */
  def fromNormalEncoderWithType[T: Encoder](outputTypeO: Option[DataType]): TypedEncoder[T] = {
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
              outputTypeO.fold{
                // typical case, normal name resolution will work
                If(IsNull(ForceNullable(path)), Literal(null), n)
              } { outputType =>
                // e.g. buried in an array
                val o = outputType.asInstanceOf[StructType].zipWithIndex.map { case (e, i) => e.name -> i }.toMap

                If(IsNull(ForceNullable(path)), Literal(null),
                  n.withNewChildren(n.children map {
                    _.transform {
                      case u: UnresolvedAttribute if o.contains(u.name) =>
                        GetStructField3(path, o(u.name))
                    }
                  })
                )
              }
            case i: InitializeJavaBean =>
              outputTypeO.fold{
                // typical case, normal name resolution will work
                If(IsNull(ForceNullable(path)), Literal(null), i)
              } { outputType =>
                // e.g. buried in an array
                val o = outputType.asInstanceOf[StructType].zipWithIndex.map{case (e,i) => e.name -> i }.toMap

                If(IsNull(ForceNullable(path)), Literal(null),
                  i.copy(setters =
                    i.setters.map{ p =>
                      (p._1, p._2.transform {
                        case u: UnresolvedAttribute if o.contains(u.name) =>
                          GetStructField3(path, o(u.name))
                      })
                    }
                  )
                )
              }
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
        val outputType = oexpr.schema

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
          val o = outputType.asInstanceOf[StructType]

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