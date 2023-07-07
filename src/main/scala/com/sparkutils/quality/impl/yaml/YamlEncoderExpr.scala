package com.sparkutils.quality.impl.yaml

import java.io.StringWriter
import java.util.Base64

import com.sparkutils.quality.QualityException
import com.sparkutils.quality.impl.MapUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.yaml.snakeyaml.{DumperOptions, Yaml}

import scala.collection.JavaConverters._
import org.yaml.snakeyaml.nodes.{MappingNode, Node, NodeTuple, ScalarNode, Tag}
import org.yaml.snakeyaml.representer.Representer

case class YamlEncoderExpr(child: Expression) extends UnaryExpression with NullIntolerant with CodegenFallback {
  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)

  lazy val valueConverter = makeValueConverter(child.dataType)

  override def nullSafeEval(input: Any): Any = {
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

  val dummyMark = null// new Mark("Dummy", 1, 1, 1, "str: Array[Char]".toCharArray, 1)

  def createScalarNode(a: Any): ScalarNode = createScalarNode(a.toString)
  def createScalarNode(a: String): ScalarNode =
    new ScalarNode(Tag.STR, a, dummyMark, dummyMark, DumperOptions.ScalarStyle.LITERAL)

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
        createScalarNode( i.getString(p) )

    case TimestampType =>
      (i: InternalRow, p: Int) =>
        createScalarNode( i.getLong(p) )

    case TimestampNTZType =>
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

    case CalendarIntervalType =>
      (i: InternalRow, p: Int) =>
        createScalarNode( i.getInterval(p) )
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
      (i: InternalRow, p: Int) => {
        val row = i.getStruct(p, st.size)
        createStructNode(st, row)
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
      (i: InternalRow, p: Int) => {
        val map = i.getMap(p)

        createMapNode(mt, map, keyType, valueType)
      }
/*
    case udt: UserDefinedType[_] =>
      makeNameConverter(udt.sqlType)
*/
    case _: NullType =>
      (i: InternalRow, p: Int) =>
        createScalarNode(null: String)

    // We don't actually hit this exception though, we keep it for understandability
    case _ => throw QualityException(s"Cannot find yaml representation for type $dataType")
  }

  private def createMapNode(mt: MapType, map: MapData, keyType: ValueConverter, valueType: ValueConverter) = {
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

  private def createStructNode(st: StructType, row: InternalRow) = {
    val tuples = st.fields.zipWithIndex.map {
      case (field, index) =>
        new NodeTuple(
          createScalarNode(field.name),
          makeStructFieldConverter(field.dataType).apply(row, index)
        )
    }
    new MappingNode(Tag.MAP, tuples.toSeq.asJava, DumperOptions.FlowStyle.FLOW)
  }

  type ValueConverter = (Any) => Node

  def makeValueConverter(dataType: DataType): ValueConverter = dataType match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType
        | FloatType | DoubleType | StringType =>
      (a: Any) =>
        createScalarNode( a )

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
      (row: Any) => {
        createStructNode(st, row.asInstanceOf[InternalRow])
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
        val map = a.asInstanceOf[MapData]
        createMapNode(mt, map, keyType, valueType)
      }
    /*
        case udt: UserDefinedType[_] =>
          makeNameConverter(udt.sqlType)
    */
    case _: NullType =>
      (a: Any) =>
        createScalarNode(null: String)

    // We don't actually hit this exception though, we keep it for understandability
    case _ => throw QualityException(s"Cannot find yaml representation for type $dataType")
  }

  override def dataType: DataType = StringType
}

/*
  lazy val valueConverter = makeValueConverter(child.dataType)

  override def nullSafeEval(input: Any): Any = {
    val writer = new StringWriter()
    val generator = new YAMLFactory().disable(Feature.WRITE_DOC_START_MARKER)
      .createGenerator(writer)

    valueConverter(generator, input)

    val r = UTF8String.fromString(writer.getBuffer.toString)
    writer.close()
    r
  }

  type ValueConverter = (YAMLGenerator, InternalRow, Int) => Unit

  def makeStructFieldConverter(dataType: DataType): ValueConverter = dataType match {
    case BooleanType =>
      (y: YAMLGenerator, i: InternalRow, p: Int) =>
        y.writeBoolean( i.getBoolean(p) )

    case ByteType =>
      (y: YAMLGenerator, i: InternalRow, p: Int) =>
        y.writeNumber( i.getByte(p).toInt )

    case ShortType =>
      (y: YAMLGenerator, i: InternalRow, p: Int) =>
        y.writeNumber( i.getShort(p).toInt )

    case IntegerType =>
      (y: YAMLGenerator, i: InternalRow, p: Int) =>
        y.writeNumber( i.getInt(p) )

    case LongType =>
      (y: YAMLGenerator, i: InternalRow, p: Int) =>
        y.writeNumber( i.getLong(p) )

    case FloatType =>
      (y: YAMLGenerator, i: InternalRow, p: Int) =>
        y.writeNumber( i.getFloat(p) )

    case DoubleType =>
      (y: YAMLGenerator, i: InternalRow, p: Int) =>
        y.writeNumber( i.getDouble(p) )

    case StringType =>
      (y: YAMLGenerator, i: InternalRow, p: Int) =>
        y.writeString( i.getString(p) )

    case TimestampType =>
      (y: YAMLGenerator, i: InternalRow, p: Int) =>
        y.writeNumber( i.getLong(p) )

    case TimestampNTZType =>
      (y: YAMLGenerator, i: InternalRow, p: Int) =>
        y.writeNumber( i.getLong(p) )

    case DateType =>
      (y: YAMLGenerator, i: InternalRow, p: Int) =>
        y.writeNumber( i.getInt(p) )

    case BinaryType =>
      (y: YAMLGenerator, i: InternalRow, p: Int) =>
        y.writeString( Base64.getEncoder.encodeToString(i.getBinary(p)) )

    case dt: DecimalType =>
      (y: YAMLGenerator, i: InternalRow, p: Int) =>
        y.writeNumber( i.getDecimal(p, dt.precision, dt.scale).toJavaBigDecimal )

    case CalendarIntervalType =>
      (y: YAMLGenerator, i: InternalRow, p: Int) =>
        y.writeObject( i.getInterval(p) )
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
      (y: YAMLGenerator, i: InternalRow, p: Int) => {
        val row = i.getStruct(p, st.size)
        y.writeStartObject()
        st.fields.zipWithIndex.foreach{
          case (field, index) =>
            y.writeFieldName(field.name)
            makeStructFieldConverter(field.dataType).apply(y, row, index)
        }
        y.writeEndObject()
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
      (y: YAMLGenerator, i: InternalRow, p: Int) => {
        val map = i.getMap(p)
        y.writeStartObject()
        map.foreach(mt.keyType, mt.valueType, (k, v) => {

          keyType.apply(y, k)
          valueType.apply(y, v)

        })
        y.writeEndObject()
      }
/*
    case udt: UserDefinedType[_] =>
      makeNameConverter(udt.sqlType)
*/
    case _: NullType =>
      (y: YAMLGenerator, i: InternalRow, p: Int) =>
        y.writeNull

    // We don't actually hit this exception though, we keep it for understandability
    case _ => throw QualityException(s"Cannot find yaml representation for type $dataType")
  }

  type ArrayAccessConverter = (YAMLGenerator, Any) => Unit

  def makeValueConverter(dataType: DataType): ArrayAccessConverter = dataType match {
    case BooleanType =>
      (y: YAMLGenerator, a: Any) =>
        y.writeBoolean( a.asInstanceOf[Boolean] )

    case ByteType =>
      (y: YAMLGenerator, a: Any) =>
        y.writeNumber( a.asInstanceOf[Byte].toInt )

    case ShortType =>
      (y: YAMLGenerator, a: Any) =>
        y.writeNumber( a.asInstanceOf[Short].toInt )

    case IntegerType =>
      (y: YAMLGenerator, a: Any) =>
        y.writeNumber( a.asInstanceOf[Int] )

    case LongType =>
      (y: YAMLGenerator, a: Any) =>
        y.writeNumber( a.asInstanceOf[Long] )

    case FloatType =>
      (y: YAMLGenerator, a: Any) =>
        y.writeNumber( a.asInstanceOf[Float] )

    case DoubleType =>
      (y: YAMLGenerator, a: Any) =>
        y.writeNumber( a.asInstanceOf[Double] )

    case StringType =>
      (y: YAMLGenerator, a: Any) =>
        y.writeString( a.toString )
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
      (y: YAMLGenerator, row: Any) => {
        y.writeStartObject()
        st.fields.zipWithIndex.foreach{
          case (field, index) =>
            y.writeFieldName(field.name)
            makeStructFieldConverter(field.dataType).apply(y, row.asInstanceOf[InternalRow], index)
        }
        y.writeEndObject()
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
      (y: YAMLGenerator, a: Any) => {
        val map = a.asInstanceOf[MapData]
        y.writeStartObject()
        map.foreach(mt.keyType, mt.valueType, (k, v) => {
          y.writeFieldId()
          keyType.apply(y, k)
          valueType.apply(y, v)
        })
        y.writeEndObject()
      }
    /*
        case udt: UserDefinedType[_] =>
          makeNameConverter(udt.sqlType)
    */
    case _: NullType =>
      (y: YAMLGenerator, a: Any) =>
        y.writeNull

    // We don't actually hit this exception though, we keep it for understandability
    case _ => throw QualityException(s"Cannot find yaml representation for type $dataType")
  }

  override def dataType: DataType = StringType
}

 */