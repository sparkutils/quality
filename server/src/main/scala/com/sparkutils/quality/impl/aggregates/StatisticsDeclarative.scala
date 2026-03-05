package com.sparkutils.quality.impl.aggregates

import com.sparkutils.quality.ResultStatisticsProvider.ResultStatisticOps
import com.sparkutils.quality.RuleSuite.defaultProbablePass
import com.sparkutils.quality.impl.aggregates.StatsRowOps.processResult
import com.sparkutils.quality.impl.aggregates.StatsTypes.{rType, rgType, rsType, setType, updateStats}
import com.sparkutils.quality.{DefaultRule, DefaultRuleInt, DisabledRule, DisabledRuleInt, Failed, FailedInt, IgnoredRule, IgnoredRuleInt, Passed, PassedInt, Probability, RuleSuiteGroupResults, RuleSuiteGroupStatistics, RuleSuiteResult, RuleSuiteStatistics, SoftFailed, SoftFailedInt}
import com.sparkutils.quality.impl.util.{Compare, Maps}
import com.sparkutils.quality.impl.util.Maps.{growMap, replaceEntry}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BinaryExpression, Expression, Literal, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.trees.{BinaryLike, UnaryLike}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData, MapData, TypeUtils}
import org.apache.spark.sql.types.{DataType, LongType, ObjectType, StructType}
import org.apache.spark.sql.catalyst.dsl.expressions._

import scala.collection.mutable

// UnsafeRow may not allow changes

object StatsTypes {
  val rsType = com.sparkutils.quality.impl.Encoders.ruleSuiteStatisticsTypedExpEnc.schema
  val rgType = com.sparkutils.quality.impl.Encoders.ruleSuiteGroupStatisticsTypedExpEnc.schema
  val setType = com.sparkutils.quality.impl.Encoders.ruleSetStatisticsTypedExpEnc.schema
  val rType = com.sparkutils.quality.impl.Encoders.ruleStatisticsTypedExpEnc.schema

  def updateStats(res: Int, row: InternalRow): Unit = res match {
    case FailedInt => row.update(1, row.getLong(1) + 1L)
    case PassedInt => row.update(2, row.getLong(2) + 1L)
    case SoftFailedInt => row.update(3, row.getLong(3) + 1L)
    case DisabledRuleInt => row.update(4, row.getLong(4) + 1L)
    case IgnoredRuleInt => row.update(5, row.getLong(5) + 1L)
    case DefaultRuleInt => row.update(6, row.getLong(6) + 1L)
    case a: Int =>
      // TODO thread a non default pass
      if (Probability(a.toDouble / PassedInt).percentage >= defaultProbablePass)
        row.update(7, row.getLong(7) + 1L)
      else
        row.update(8, row.getLong(8) + 1L)
  }

  def mergeStats(into: InternalRow, from: InternalRow): Unit =
    for(i <- 1 until 8) {
      into.update(i, into.getLong(i) + from.getLong(i))
    }

}


case class StatsRowOps(level: String, statsMapOffset: Int, inputRowResult: Any => Int, statsDefaultNestedType: InternalRow,
                       statsNestedType: DataType, statsRowUpdate: InternalRow => Unit, statsBuildWhenNewMap: (InternalRow, MapData) => InternalRow,
                       nextInputLevels: Any => MapData, nextInputRowSize: Int, nextStatMap: InternalRow => MapData,
                       nextStatType: StructType, nextStatsBuildWhenNewMap: (InternalRow, MapData) => InternalRow)

object StatsRowOps {

  lazy val defaultSuiteStats = Literal.default(rsType).value.asInstanceOf[InternalRow]
  lazy val defaultSetStats = Literal.default(setType).value.asInstanceOf[InternalRow]
  lazy val defaultRuleStats = Literal.default(rType).value.asInstanceOf[InternalRow]

  val suiteMapF = (suite: InternalRow, m: MapData) => {
    InternalRow(suite.getLong(0), suite.getLong(1),suite.getLong(2),suite.getLong(3),suite.getLong(4),
      suite.getLong(5),suite.getLong(6),suite.getLong(7),suite.getLong(8), suite.getLong(9), m)
  }
  val setMapF = (set: InternalRow, m: MapData) => {
    InternalRow(set.getLong(0), set.getLong(1),set.getLong(2),set.getLong(3),set.getLong(4),
      set.getLong(5),set.getLong(6),set.getLong(7),set.getLong(8), m)
  }
  val emptyAr = new GenericArrayData(Array.ofDim[Any](0))
  val emptyMap = new ArrayBasedMapData(emptyAr, emptyAr)

  val config = Seq(
    StatsRowOps(level = "group_to_suite", statsMapOffset = 0,
      inputRowResult = _.asInstanceOf[InternalRow].getInt(1), statsDefaultNestedType = defaultSuiteStats,
      statsNestedType = rsType, statsRowUpdate = row => row.update(9, row.getLong(9) + 1L),
      statsBuildWhenNewMap = (curGroup, m) => InternalRow(m, curGroup.getLong(1)),
      nextInputLevels = row => row.asInstanceOf[InternalRow].getMap(2), nextInputRowSize = 2,
      nextStatMap = _.getMap(10), nextStatType = setType, nextStatsBuildWhenNewMap = suiteMapF
    ),
    StatsRowOps(level = "suite_to_set", statsMapOffset = 10,
      inputRowResult = _.asInstanceOf[InternalRow].getInt(0), statsDefaultNestedType = defaultSetStats,
      statsNestedType = setType, statsRowUpdate = _ => (), statsBuildWhenNewMap =  suiteMapF,
      nextInputLevels = row => row.asInstanceOf[InternalRow].getMap(1), nextInputRowSize = -1,
      nextStatMap = _.getMap(9), nextStatType = rType, nextStatsBuildWhenNewMap = setMapF),
    // most of the bit below are ignored for a rule set level result
    StatsRowOps(level = "set_to_rule", statsMapOffset = 9,
      inputRowResult = _.asInstanceOf[Int], statsDefaultNestedType = defaultRuleStats,
      statsNestedType = rType, statsRowUpdate = _ => (), statsBuildWhenNewMap = setMapF,
      nextInputLevels = _ => emptyMap, nextInputRowSize = -1,
      nextStatMap = _ => emptyMap, nextStatType = rType, nextStatsBuildWhenNewMap = (row, _) => row)
  )

  def processResult(curGroup: InternalRow, row: InternalRow) = {
    curGroup.update(1, curGroup.getLong(1) + 1L)

    def process(id: Long, row: Any, cur: InternalRow, stats: Seq[StatsRowOps]): (InternalRow, Boolean) =
      if (stats.isEmpty) {
        val head = stats.head
        import head._

        updateStats(row.asInstanceOf[Int], cur)
        (cur, false)
      } else {
        val (head, rest) = (stats.head, stats.tail)
        import head._

        val m = cur.getMap(statsMapOffset)
        var rs_i = -1
        var i = 0
        while (rs_i == -1 && i < m.numElements()) {
          if (m.keyArray().getLong(i) == id) {
            rs_i = i
          }
        }
        var createdNewMap = false
        val rs =
          if (rs_i > -1)
            m.valueArray().get(rs_i, statsNestedType).asInstanceOf[InternalRow]
          else {
            val r = statsDefaultNestedType.copy()
            r.setLong(0, id) // always the first field in the stats

            createdNewMap = true
            r
          }

        /*val nm =
          if (rs_i > -1)
            m
          else {
            val nm = growMap(m, LongType, statsNestedType,
              (id, {
                val r = statsDefaultNestedType.copy()
                r.setLong(0, id) // always the first field in the stats
                r
              }))
            rs_i = m.numElements()
            createdNewMap = true
            nm
          }
        val rs = nm.valueArray().get(rs_i, statsNestedType).asInstanceOf[InternalRow]

         */
        statsRowUpdate(rs)
        updateStats(inputRowResult(row), rs)

        val nextSM = nextStatMap(rs)
        val resultRows = nextInputLevels(row)

        val newRows =
          for { i <- 0 until resultRows.numElements() }
            yield
              process(resultRows.keyArray().getLong(i),
                if (nextInputRowSize > 0)
                  resultRows.valueArray().getStruct(i, nextInputRowSize)
                else
                  // it's the rule level result
                  resultRows.valueArray().getInt(i)
                , rs, rest) // this should be the correct row, so we should be inside already...

        // if any new rows were added below then we'll have to re-create all above
        val createdSubMap = newRows.exists(_._2)
        lazy val newRow: InternalRow =
          {
            // newRows may be updated or added, but there are also possible left-overs
            val newMap = mutable.Map.empty[Long, InternalRow]// TODO perhaps an array is quicker for smaller volumes
            for{
              i <- 0 until nextSM.numElements()
            } {
              newMap.put(nextSM.keyArray().getLong(i), nextSM.valueArray().getStruct(i, nextStatType.length))
            }
            for{
              i <- newRows.indices
            } {
              newMap.put(newRows(i)._1.getLong(0), newRows(i)._1)
            }
            val keys = new GenericArrayData(newMap.keys.toArray)
            val values = new GenericArrayData(newMap.values.toArray)
            nextStatsBuildWhenNewMap(rs, new ArrayBasedMapData(keys, values))
          }

        (if (rest.isEmpty || !(createdNewMap || createdSubMap))
            rs
          else
            newRow,
          createdNewMap || createdSubMap)
        /*else
          if (!(createdNewMap || createdSubMap))
            // updated inplace
            (cur, false)
          else {
            val tnm =
              if (createdSubMap) {
                // need to re-incorporate
                val n = newRow // n because intellij debug keeps putting StatRowOps in there
                replaceEntry(nm, LongType, statsNestedType, rs_i, (id, newRow))
              } else
                nm

            (statsBuildWhenNewMap(cur, tnm), true)
          }*/
      }

    val res = process(row.getLong(0), row, curGroup, config)
    if (res._2) {
      // some form of map change needed below, so it must cascade up
      val id = row.getLong(0)
      val head = config.head
      import head._
      // either we need to re-integrate it or it was brand new
      val m = curGroup.getMap(statsMapOffset)
      var rs_i = -1
      var i = 0
      while (rs_i == -1 && i < m.numElements()) {
        if (m.keyArray().getLong(i) == row.getLong(0)) {
          rs_i = i
        }
      }
      val nm =
        if (rs_i > -1) {
          // it needs to be replaced
          replaceEntry(m, LongType, statsNestedType, rs_i, (id, res._1))
        } else {
          // it was never there
          val nm = growMap(m, LongType, statsNestedType,
            (id, res._1))
          rs_i = m.numElements()
          nm
        }

      statsBuildWhenNewMap(curGroup, nm)
    } else
      res._1

  }
}

case class ProcessStatistics(children: Seq[Expression]) extends Expression with CodegenFallback {

  lazy val Seq(grp, col, groupSer, groupDer, ruleDer) = children

  override def eval(input: InternalRow): Any = {
    val curGroup = grp.eval(input).asInstanceOf[InternalRow]
    val rcol = col.eval(input)
    if (rcol == null)
      curGroup
    else {
      val row = rcol.asInstanceOf[InternalRow]
      processResult(curGroup: InternalRow, row: InternalRow)
      /*val lgrp = groupDer.eval(cur).asInstanceOf[RuleSuiteGroupStatistics]
      val rgrp = ruleDer.eval(row).asInstanceOf[RuleSuiteResult]
      val r = lgrp.process(rgrp)
      groupSer.eval(InternalRow(r))*/
    }
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)

  override def dataType: DataType = rgType

  override def nullable: Boolean = false
}

case class MergeStatistics(children: Seq[Expression]) extends Expression with CodegenFallback {

  lazy val Seq(left, right, groupSer, groupDer) = children

  override def eval(input: InternalRow): Any = {
    val lgrp = groupDer.eval(left.eval(input).asInstanceOf[InternalRow]).asInstanceOf[RuleSuiteGroupStatistics]
    val rgrp = groupDer.eval(right.eval(input).asInstanceOf[InternalRow]).asInstanceOf[RuleSuiteGroupStatistics]
    val r = lgrp.combine(rgrp)
    groupSer.eval(InternalRow(r))
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)

  override def dataType: DataType = rgType

  override def nullable: Boolean = false
}

/**
 *
 * @param left the column
 * @param right the objSerializer
 */
case class StatisticsDeclarative(children: Seq[Expression]) extends DeclarativeAggregate {

  lazy val Seq(col, groupSer, groupDer, ruleDer) = children

  override def checkInputDataTypes(): TypeCheckResult =
    if (Compare.equalsIgnoreCaseAndNullability(col.dataType, com.sparkutils.quality.impl.types.ruleSuiteResultType))
      TypeCheckResult.TypeCheckSuccess
    else
      TypeCheckResult.TypeCheckFailure(s"Statistics accepts RuleSuiteResult columns but was provided ${col.dataType}")

  val sumDataType = rgType
  lazy val sum = AttributeReference("sum", sumDataType)()

  override val initialValues: Seq[Expression] = Seq(new Literal(
    groupSer.eval(InternalRow(RuleSuiteGroupStatistics())),
    sumDataType))
  override val updateExpressions: Seq[Expression] = Seq(
    ProcessStatistics(Seq(sum, col, groupSer, groupDer, ruleDer))
  )
  override val mergeExpressions: Seq[Expression] = Seq(
    MergeStatistics(Seq(sum.left, sum.right, groupSer, groupDer))
  )
  override val evaluateExpression: Expression = sum

  override def aggBufferAttributes: Seq[AttributeReference] = Seq(sum)

  override def nullable: Boolean = false

  override def dataType: DataType = sumDataType

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)
}
