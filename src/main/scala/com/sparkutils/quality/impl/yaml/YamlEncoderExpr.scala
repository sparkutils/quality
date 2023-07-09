package com.sparkutils.quality.impl.yaml

import java.io.StringWriter
import java.util.Base64

import com.sparkutils.quality.impl.MapUtils
import com.sparkutils.quality.impl.util.Arrays
import org.apache.spark.sql.QualityYamlExt.{makeConverterExt, makeStructFieldConverterExt}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.yaml.snakeyaml.nodes._
import org.yaml.snakeyaml.representer.Representer
import org.yaml.snakeyaml.{DumperOptions, Yaml}

import scala.collection.JavaConverters._

object QualityYamlEncoding {

  type Converter = (Any) => Node
  type ValueConverter = PartialFunction[DataType, Converter]

  type StructValueConverter = PartialFunction[DataType, (InternalRow, Int) => Node]

  val dummyMark = null

  def createNullNode: ScalarNode = createScalarNode(null)

  def createScalarNode(a: Any): ScalarNode =
    if (a == null)
      new ScalarNode(Tag.NULL, "null", dummyMark, dummyMark, DumperOptions.ScalarStyle.PLAIN)
    else
      new ScalarNode(Tag.STR, a.toString, dummyMark, dummyMark, DumperOptions.ScalarStyle.PLAIN)


  def makeStructFieldConverter: StructValueConverter = {
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
      val keyType = makeValueConverter.applyOrElse(mt.keyType, makeConverterExt)
      val valueType = makeValueConverter.applyOrElse(mt.valueType, makeConverterExt)
      (i: InternalRow, p: Int) => {
        val map = i.getMap(p)

        createMapNode(mt, map, keyType, valueType)
      }

    case _: NullType =>
      (i: InternalRow, p: Int) =>
        createScalarNode(null)
  }

  private def createSequenceNode(at: ArrayType) = {
    val elementConverter = makeValueConverter.applyOrElse(at.elementType, makeConverterExt)
    (ar: ArrayData) => {
      if (ar == null)
        createNullNode
      else {
        val vals = Arrays.mapArray[Node](ar, at.elementType, elementConverter(_))
        new SequenceNode(Tag.SEQ, vals.toSeq.asJava, DumperOptions.FlowStyle.FLOW)
      }
    }
  }

  private def createMapNode(mt: MapType, map: MapData, keyType: Any => Node, valueType: Any => Node) =
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
      makeStructFieldConverter.applyOrElse(f.dataType, makeStructFieldConverterExt))

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

  def makeValueConverter: ValueConverter = {
    case BooleanType | ByteType | ShortType | IntegerType | LongType
         | FloatType | DoubleType | StringType | TimestampType | DateType | _: DecimalType =>
      (a: Any) =>
        createScalarNode( a )

    case BinaryType =>
      (a: Any) =>
        createScalarNode( Base64.getEncoder.encodeToString(a.asInstanceOf[Array[Byte]]))

    case st: StructType =>
      val sf = createStructNode(st)
      (a: Any) =>
        sf(a.asInstanceOf[InternalRow])

    case at: ArrayType =>
      val af = createSequenceNode(at)
      (a: Any) =>
        af(a.asInstanceOf[ArrayData])

    case mt: MapType =>
      val keyType = makeValueConverter.applyOrElse(mt.keyType, makeConverterExt)
      val valueType = makeValueConverter.applyOrElse(mt.valueType, makeConverterExt)
      (a: Any) => {
        val map = a.asInstanceOf[MapData]
        createMapNode(mt, map, keyType, valueType)
      }

    case _: NullType =>
      (a: Any) =>
        createNullNode
  }

}

case class YamlEncoderExpr(child: Expression) extends UnaryExpression with CodegenFallback {
  import QualityYamlEncoding._
  import org.apache.spark.sql.QualityYamlExt._

  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)

  lazy val valueConverter: Converter =
    makeValueConverter.applyOrElse(child.dataType, makeConverterExt)

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

  override def dataType: DataType = StringType
}
