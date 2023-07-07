package com.sparkutils.quality.impl.yaml

import java.io.{StringReader, StringWriter}
import java.util.Base64

import com.sparkutils.quality.QualityException
import com.sparkutils.quality.impl.MapUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, MapData}
import org.apache.spark.sql.qualityFunctions.InputTypeChecks
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.yaml.snakeyaml.nodes._
import org.yaml.snakeyaml.representer.Representer
import org.yaml.snakeyaml.{DumperOptions, Yaml}

import scala.collection.JavaConverters._

case class YamlDecoderExpr(child: Expression, dataType: DataType) extends UnaryExpression with NullIntolerant with CodegenFallback with InputTypeChecks {
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

  def makeValueConverter(dataType: DataType): Converter = dataType match {
    case BooleanType =>
      (a: Node) =>
        a.asInstanceOf[ScalarNode].getValue.toBoolean
    case ByteType =>
      (a: Node) =>
        a.asInstanceOf[ScalarNode].getValue.toByte
    case ShortType =>
      (a: Node) =>
        a.asInstanceOf[ScalarNode].getValue.toShort
    case IntegerType =>
      (a: Node) =>
        a.asInstanceOf[ScalarNode].getValue.toInt
    case LongType =>
      (a: Node) =>
        a.asInstanceOf[ScalarNode].getValue.toLong
    case FloatType =>
      (a: Node) =>
        a.asInstanceOf[ScalarNode].getValue.toFloat
    case DoubleType =>
      (a: Node) =>
        a.asInstanceOf[ScalarNode].getValue.toDouble
    case StringType =>
      (a: Node) =>
        UTF8String.fromString( a.asInstanceOf[ScalarNode].getValue )
/*
    case TimestampType =>
      (y: YAMLGenerator, a: Any) =>
        y.writeNumber( i.getLong(p) )

    case TimestampNTZType =>
      (y: YAMLGenerator, a: Any) =>
        y.writeNumber( i.getLong(p) )

    case DateType =>
      (y: YAMLGenerator, a: Any) =>
        y.writeNumber( i.getInt(p) )

    case BinaryType =>
      (y: YAMLGenerator, a: Any) =>
        y.writeString( Base64.getEncoder.encodeToString(i.getBinary(p)) )

    case dt: DecimalType =>
      (y: YAMLGenerator, a: Any) =>
        y.writeNumber( i.getDecimal(p, dt.precision, dt.scale).toJavaBigDecimal )

    case CalendarIntervalType =>
      (y: YAMLGenerator, a: Any) =>
        y.writeObject( i.getInterval(p) )*/
    /*
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
    */
    case st: StructType =>
      val converters = st.fields.map(f => makeValueConverter(f.dataType))
      (a: Node) => {
        val map = a.asInstanceOf[MappingNode]
        val tuples = map.getValue
        val values =
          tuples.asScala.zipWithIndex.map{
            case (tuple, index) =>
              val name = tuple.getKeyNode.asInstanceOf[ScalarNode]
              if (name.getValue != st.fields(index).name) {
                throw QualityException(s"Could not load yaml, expected field name ${st.fields(index).name} but got ${name.getValue}")
              }

              converters(index)(tuple.getValueNode)
          }
        InternalRow(values:_*)
      }

    /*
  case at: ArrayType =>
    val elementConverter = makeConverter(at.elementType)
    (parser: JsonParser) => parseJsonToken[ArrayData](parser, dataType) {
      case _ => ???
      //  case START_ARRAY => convertArray(parser, elementConverter)
    }
*/
    case mt: MapType =>
      val keyType = makeValueConverter(mt.keyType)
      val valueType = makeValueConverter(mt.valueType)
      (a: Any) => {
        val map = a.asInstanceOf[MappingNode]
        val tuples = map.getValue
        val smap =
          tuples.asScala.map{
            case tuple =>
              keyType(tuple.getKeyNode) -> valueType(tuple.getValueNode)
          }.toMap
        ArrayBasedMapData.apply(smap)
      }
    /*
        case udt: UserDefinedType[_] =>
          makeNameConverter(udt.sqlType)
    */
    case _: NullType =>
      (a: Any) =>
       null

    // We don't actually hit this exception though, we keep it for understandability
    case _ => throw QualityException(s"Cannot find yaml representation for type $dataType")
  }

  override def inputDataTypes: Seq[Seq[DataType]] = Seq(Seq(StringType))
}
