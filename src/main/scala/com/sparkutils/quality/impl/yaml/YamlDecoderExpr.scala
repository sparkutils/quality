package com.sparkutils.quality.impl.yaml

import com.sparkutils.quality.impl.YamlDecoder

import java.io.StringReader
import java.util.Base64
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.shim.expressions.InputTypeChecks
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.yaml.snakeyaml.nodes._

import scala.collection.JavaConverters._

object QualityYamlDecoding extends Logging {

  type NodeConverter = PartialFunction[DataType, Node => Any]

  implicit class NodeOps(a: Node){
    def scalar[T](convert: String => T) = {
      val s = a.asInstanceOf[ScalarNode]
      if (s.getTag == Tag.NULL)
        null
      else
        convert(s.getValue)
    }
  }

  import org.apache.spark.sql.QualityYamlExt.makeNodeConverterExt
  def makeNodeConverter: NodeConverter = {
    case BooleanType =>
      (a: Node) =>
        a.scalar(_.toBoolean)

    case ByteType =>
      (a: Node) =>
        a.scalar(_.toByte)

    case ShortType =>
      (a: Node) =>
        a.scalar(_.toShort)

    case IntegerType =>
      (a: Node) =>
        a.scalar(_.toInt)

    case LongType =>
      (a: Node) =>
        a.scalar(_.toLong)

    case FloatType =>
      (a: Node) =>
        a.scalar(_.toFloat)

    case DoubleType =>
      (a: Node) =>
        a.scalar(_.toDouble)

    case StringType =>
      (a: Node) =>
        a.scalar( UTF8String.fromString( _ ) )

    case TimestampType =>
      (a: Node) =>
        a.scalar(_.toLong)

    case DateType =>
      (a: Node) =>
        a.scalar(_.toInt)

    case BinaryType =>
      (a: Node) =>
        a.scalar( Base64.getDecoder.decode( _ ) )

    case dt: DecimalType =>
      (a: Node) =>
        a.scalar( s =>
          Decimal( BigDecimal( s ), dt.precision, dt.scale )
        )

    /*

    case CalendarIntervalType =>


        case TimestampNTZType =>
          (y: YAMLGenerator, a: Any) =>
            y.writeNumber( i.getLong(p) )

            case ym: YearMonthIntervalType => (parser: JsonParser) =>
              parseJsonName[Integer](parser, dataType) {
                case s =>
                  val expr = Cast(Literal(s), ym)
                  Integer.valueOf(expr.eval(EmptyRow).asInstanceOf[Int])
              }

            case dt: DayTimeIntervalType => (parser: JsonParser) =>
              parseJsonName[java.lang.Long](parser, dataType) {
                case s =>
                  val expr = Cast(Literal(s), dt)
                  java.lang.Long.valueOf(expr.eval(EmptyRow).asInstanceOf[Long])
              }
    /*
        case udt: UserDefinedType[_] =>
          makeNameConverter(udt.sqlType)
    */

        */
    case st: StructType =>
      val converters = st.fields.map(f => makeNodeConverter.applyOrElse(f.dataType, makeNodeConverterExt))
      (a: Node) => {
        if (a.getTag == Tag.NULL)
          null
        else {
          val map = a.asInstanceOf[MappingNode]
          val tuples = map.getValue
          val values =
            tuples.asScala.zipWithIndex.map {
              case (tuple, index) =>
                val name = tuple.getKeyNode.asInstanceOf[ScalarNode]
                if (name.getValue != st.fields(index).name) {
                  logWarning(s"Could not load yaml, expected field name ${st.fields(index).name} but got ${name.getValue}, returning null")
                  return { case _ => (a: Node) => null }
                }

                converters(index)(tuple.getValueNode)
            }
          InternalRow(values: _*)
        }
      }

    case at: ArrayType =>
      val elementConverter = makeNodeConverter.applyOrElse(at.elementType, makeNodeConverterExt)
      (a: Node) => {
        if (a.getTag == Tag.NULL)
          null
        else {
          val seq = a.asInstanceOf[SequenceNode]
          val vals = seq.getValue.asScala.map(elementConverter)
          ArrayData.toArrayData(vals)
        }
      }

    case mt: MapType =>
      val keyType = makeNodeConverter.applyOrElse(mt.keyType, makeNodeConverterExt)
      val valueType = makeNodeConverter.applyOrElse(mt.valueType, makeNodeConverterExt)
      (a: Node) => {
        if (a.getTag == Tag.NULL)
          null
        else {
          val map = a.asInstanceOf[MappingNode]

          val tuples = map.getValue
          val smap =
            tuples.asScala.map {
              case tuple =>
                keyType(tuple.getKeyNode) -> valueType(tuple.getValueNode)
            }.toMap
          ArrayBasedMapData.apply(smap)
        }
      }

    case _: NullType =>
      (a: Node) =>
        null

  }

}

case class YamlDecoderExpr(child: Expression, dataType: DataType) extends UnaryExpression with NullIntolerant with
  CodegenFallback with InputTypeChecks with Logging {
  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)

  import QualityYamlDecoding._
  import org.apache.spark.sql.QualityYamlExt._

  lazy val valueConverter = makeNodeConverter.applyOrElse(dataType, makeNodeConverterExt)

  override def nullSafeEval(input: Any): Any = {
    val generator = YamlDecoder.yaml
    val reader = new StringReader(input.toString)

    val node = generator.compose(reader)
    reader.close()

    valueConverter(node)
  }

  override def inputDataTypes: Seq[Seq[DataType]] = Seq(Seq(StringType))
}
