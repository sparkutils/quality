package com.sparkutils.quality.impl.yaml

import java.io.{StringReader, StringWriter}
import java.util.Base64

import com.sparkutils.quality.QualityException
import com.sparkutils.quality.impl.MapUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, MapData}
import org.apache.spark.sql.qualityFunctions.InputTypeChecks
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.yaml.snakeyaml.nodes._
import org.yaml.snakeyaml.representer.Representer
import org.yaml.snakeyaml.{DumperOptions, Yaml}

import scala.collection.JavaConverters._

case class YamlDecoderExpr(child: Expression, dataType: DataType) extends UnaryExpression with NullIntolerant with
  CodegenFallback with InputTypeChecks with Logging {
  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)

  lazy val valueConverter = makeValueConverter(dataType)

  override def nullSafeEval(input: Any): Any = {
    val generator = new Yaml()
    val reader = new StringReader(input.toString)

    val node = generator.compose(reader)
    reader.close()

    valueConverter(node)
  }

  type Converter = Node => Any

  val dummyMark = null

  implicit class NodeOps(a: Node){
    def scalar[T](convert: String => T) = {
      val s = a.asInstanceOf[ScalarNode]
      if (s.getTag == Tag.NULL)
        null
      else
        convert(s.getValue)
    }
  }

  def makeValueConverter(dataType: DataType): Converter = dataType match {
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
      def getField(tuples: Seq[NodeTuple])( name: String) =
        tuples.find(_.getKeyNode.asInstanceOf[ScalarNode].getValue == name)

      def getValue(node: NodeTuple) =
        node.getValueNode.asInstanceOf[ScalarNode].getValue

      (a: Node) => {
        val map = a.asInstanceOf[MappingNode]
        val tuples = map.getValue.asScala
        val f = getField(tuples) _
        val r =
          for{
            days <- f("days").map(getValue(_).toInt)
            microseconds <- f("microseconds").map(getValue(_).toLong)
            months <- f("months").map(getValue(_).toInt)
          } yield
            new CalendarInterval(months, days, microseconds)
        r.orNull // not really exception territory
      }

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
      val converters = st.fields.map(f => makeValueConverter(f.dataType))
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
                  return (a: Node) => null
                }

                converters(index)(tuple.getValueNode)
            }
          InternalRow(values: _*)
        }
      }

    case at: ArrayType =>
      val elementConverter = makeValueConverter(at.elementType)
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
      val keyType = makeValueConverter(mt.keyType)
      val valueType = makeValueConverter(mt.valueType)
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

    // We don't actually hit this exception though, we keep it for understandability
    case _ => throw QualityException(s"Cannot find yaml representation for type $dataType")
  }

  override def inputDataTypes: Seq[Seq[DataType]] = Seq(Seq(StringType))
}
