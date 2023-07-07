package com.sparkutils.quality.impl.yaml

import java.io.StringWriter
import java.util.Base64

import com.sparkutils.quality.QualityException
import com.sparkutils.quality.impl.MapUtils
import com.sparkutils.quality.impl.util.Arrays
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{UTF8String, CalendarInterval}
import org.yaml.snakeyaml.{DumperOptions, Yaml}

import scala.collection.JavaConverters._
import org.yaml.snakeyaml.nodes.{MappingNode, Node, NodeTuple, ScalarNode, SequenceNode, Tag}
import org.yaml.snakeyaml.representer.Representer

case class YamlEncoderExpr(child: Expression) extends UnaryExpression with CodegenFallback {
  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)

  lazy val valueConverter = makeValueConverter(child.dataType)

  override def eval(inputRow: InternalRow): Any = {
    val input = child.eval(inputRow)

    val writer = new StringWriter()
    val options = new DumperOptions()
    import org.yaml.snakeyaml.DumperOptions
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.FLOW)

    val representer = new Representer(options);
    val generator = new Yaml(representer, options)

    val node = valueConverter(input)

    generator.serialize(node, writer)

    val str = writer.getBuffer.toString
    writer.close()
    UTF8String.fromString(str)
  }

  type StructValueConverter = (InternalRow, Int) => Node

  val dummyMark = null

  def createNullNode: ScalarNode = createScalarNode(null)

  def createScalarNode(a: Any): ScalarNode =
    if (a == null)
      new ScalarNode(Tag.NULL, "null", dummyMark, dummyMark, DumperOptions.ScalarStyle.PLAIN)
    else
      new ScalarNode(Tag.STR, a.toString, dummyMark, dummyMark, DumperOptions.ScalarStyle.PLAIN)

  def makeStructFieldConverter(dataType: DataType): StructValueConverter = dataType match {
    case BooleanType =>
      (i: InternalRow, p: Int) =>
        createScalarNode( i.getBoolean(p) )

    case ByteType =>
      (i: InternalRow, p: Int) =>
        createScalarNode( i.getByte(p).toInt )

    case ShortType =>
      (i: InternalRow, p: Int) =>
        createScalarNode( i.getShort(p).toInt )

    case IntegerType =>
      (i: InternalRow, p: Int) =>
        createScalarNode( i.getInt(p) )

    case LongType =>
      (i: InternalRow, p: Int) =>
        createScalarNode( i.getLong(p) )

    case FloatType =>
      (i: InternalRow, p: Int) =>
        createScalarNode( i.getFloat(p) )

    case DoubleType =>
      (i: InternalRow, p: Int) =>
        createScalarNode( i.getDouble(p) )

    case StringType =>
      (i: InternalRow, p: Int) =>
        createScalarNode( i.getUTF8String(p) )

    case TimestampType =>
      (i: InternalRow, p: Int) =>
        createScalarNode( i.getLong(p) )

    case DateType =>
      (i: InternalRow, p: Int) =>
        createScalarNode( i.getInt(p) )

    case BinaryType =>
      (i: InternalRow, p: Int) =>
        createScalarNode( Base64.getEncoder.encodeToString(i.getBinary(p)) )

    case dt: DecimalType =>
      (i: InternalRow, p: Int) =>
        createScalarNode( i.getDecimal(p, dt.precision, dt.scale).toJavaBigDecimal )

    /*

    case CalendarIntervalType =>
      (i: InternalRow, p: Int) =>
        createIntervalNode( i.getInterval(p) )

can't support until much later or move this out into compat, must also be in sql for UDTs...
  case TimestampNTZType =>
    (i: InternalRow, p: Int) =>
      createScalarNode( i.getLong(p) )


    case ym: YearMonthIntervalType =>
      (i: InternalRow, p: Int) => null
      (parser: JsonParser) =>
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


    case udt: UserDefinedType[_] =>
      makeValueConverter(udt.sqlType)

      */

    case st: StructType =>
      val sf = createStructNode(st)
      (i: InternalRow, p: Int) => {
        val row = i.getStruct(p, st.size)
        sf(row)
      }

    case at: ArrayType =>
      val af = createSequenceNode(at)
      (i: InternalRow, p: Int) => {
        val ar = i.getArray(p)
        af(ar)
      }

    case mt: MapType =>
      val keyType = makeValueConverter(mt.keyType)
      val valueType = makeValueConverter(mt.valueType)
      (i: InternalRow, p: Int) => {
        val map = i.getMap(p)

        createMapNode(mt, map, keyType, valueType)
      }

    case _: NullType =>
      (i: InternalRow, p: Int) =>
        createScalarNode(null)

    // We don't actually hit this exception though, we keep it for understandability
    case _ => throw QualityException(s"Cannot find yaml representation for type $dataType")
  }

  private def createSequenceNode(at: ArrayType) = {
    val elementConverter = makeValueConverter(at.elementType)
    (ar: ArrayData) => {
      if (ar == null)
        createNullNode
      else {
        val vals = Arrays.mapArray[Node](ar, at.elementType, elementConverter(_))
        new SequenceNode(Tag.SEQ, vals.toSeq.asJava, DumperOptions.FlowStyle.FLOW)
      }
    }
  }

  private def createMapNode(mt: MapType, map: MapData, keyType: ValueConverter, valueType: ValueConverter) =
    if (map == null)
      createNullNode
    else {
      val smap = MapUtils.toScalaMap(map, mt.keyType, mt.valueType)
      val tuples =
        smap.map { case (k, v) =>
          new NodeTuple(
            keyType(k),
            valueType(v)
          )
        }
      new MappingNode(Tag.MAP, tuples.toSeq.asJava, DumperOptions.FlowStyle.FLOW)
    }

  private def createStructNode(st: StructType) = {
    val converters = st.fields.map(f =>
      makeStructFieldConverter(f.dataType))

    (row: InternalRow) => {
      if (row == null)
        createScalarNode(null)
      else {
        val tuples = st.fields.zipWithIndex.map {
          case (field, index) =>
            new NodeTuple(
              createScalarNode(field.name),
              converters(index)(row, index)
            )
        }
        new MappingNode(Tag.MAP, tuples.toSeq.asJava, DumperOptions.FlowStyle.FLOW)
      }
    }
  }

  type ValueConverter = (Any) => Node

  def makeValueConverter(dataType: DataType): ValueConverter = dataType match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType
        | FloatType | DoubleType | StringType | TimestampType | DateType | _: DecimalType =>
      (a: Any) =>
        createScalarNode( a )

    case BinaryType =>
      (a: Any) =>
        createScalarNode( Base64.getEncoder.encodeToString(a.asInstanceOf[Array[Byte]]))


    /*

    case CalendarIntervalType =>
      (a: Any) => {
        val interval = a.asInstanceOf[CalendarInterval]
        createIntervalNode(interval)
      }

  private def createIntervalNode(interval: CalendarInterval) = {
    val tuples = Seq(
      new NodeTuple(
        createScalarNode("days"),
        createScalarNode(interval.days)
      ),
      new NodeTuple(
        createScalarNode("microseconds"),
        createScalarNode(interval.microseconds)
      ),
      new NodeTuple(
        createScalarNode("months"),
        createScalarNode(interval.months)
      )
    )

    new MappingNode(Tag.MAP, tuples.asJava, DumperOptions.FlowStyle.FLOW)
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
    */
    case st: StructType =>
      val sf = createStructNode(st)
      (a: Any) =>
        sf(a.asInstanceOf[InternalRow])

    case at: ArrayType =>
      val af = createSequenceNode(at)
      (a: Any) =>
        af(a.asInstanceOf[ArrayData])

    case mt: MapType =>
      val keyType = makeValueConverter(mt.keyType)
      val valueType = makeValueConverter(mt.valueType)
      (a: Any) => {
        val map = a.asInstanceOf[MapData]
        createMapNode(mt, map, keyType, valueType)
      }
    /*
        case udt: UserDefinedType[_] =>
          makeNameConverter(udt.sqlType)
    */
    case _: NullType =>
      (a: Any) =>
        createNullNode

    // We don't actually hit this exception though, we keep it for understandability
    case _ => throw QualityException(s"Cannot find yaml representation for type $dataType")
  }

  override def dataType: DataType = StringType
}
