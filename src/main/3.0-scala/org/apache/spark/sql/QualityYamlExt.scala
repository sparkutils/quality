package org.apache.spark.sql

import com.sparkutils.quality.QualityException
import com.sparkutils.quality.impl.yaml.QualityYamlDecoding._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{CalendarIntervalType, UserDefinedType}
import org.yaml.snakeyaml.nodes.{MappingNode, Node, NodeTuple, ScalarNode, Tag}
import com.sparkutils.quality.impl.yaml.QualityYamlEncoding._
import org.apache.spark.unsafe.types.CalendarInterval
import org.yaml.snakeyaml.DumperOptions

import scala.collection.JavaConverters._

object QualityYamlExt {

  private def createIntervalNode(interval: CalendarInterval) =
    if (interval eq null)
      createNullNode
    else {
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


  def makeConverterExt: ValueConverter = {

    case CalendarIntervalType =>
      (a: Any) =>
        createIntervalNode(a.asInstanceOf[CalendarInterval])

    case udt: UserDefinedType[_] =>
      makeValueConverter.applyOrElse(udt.sqlType, makeConverterExt)

    case dataType => throw QualityException(s"Cannot find yaml representation for type $dataType")
  }

  def makeStructFieldConverterExt: StructValueConverter = {

    case CalendarIntervalType =>
      (i: InternalRow, p: Int) =>
        createIntervalNode(i.getInterval(p))

    case udt: UserDefinedType[_] =>
      // recurse ever?
      makeStructFieldConverter.applyOrElse(udt.sqlType, makeStructFieldConverterExt)

    case dataType => throw QualityException(s"Cannot find yaml representation for type $dataType")
  }

  def makeNodeConverterExt: NodeConverter = {
    case CalendarIntervalType =>
      def getField(tuples: Seq[NodeTuple])( name: String) =
        tuples.find(_.getKeyNode.asInstanceOf[ScalarNode].getValue == name)

      def getValue(node: NodeTuple) =
        node.getValueNode.asInstanceOf[ScalarNode].getValue

      (a: Node) =>
        if (a.getTag == Tag.NULL)
          null
        else {
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

    case udt: UserDefinedType[_] =>
      // recurse ever?
      makeNodeConverter.applyOrElse(udt.sqlType, makeNodeConverterExt)

    case dataType => throw QualityException(s"Cannot find yaml representation for type $dataType")
  }
}
