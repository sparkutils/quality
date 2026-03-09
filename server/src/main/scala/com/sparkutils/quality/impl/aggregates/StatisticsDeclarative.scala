package com.sparkutils.quality.impl.aggregates

import com.sparkutils.quality.RuleSuite.defaultProbablePass
import com.sparkutils.quality.impl.aggregates.StatsRowOps.processResult
import com.sparkutils.quality.impl.aggregates.StatsTypes.{mergeStats, rType, rgType, rsType, setType, updateStats}
import com.sparkutils.quality.{DefaultRuleInt, DisabledRuleInt, FailedInt, IgnoredRuleInt, PassedInt, Probability, RuleSuiteGroupStatistics, SoftFailedInt}
import com.sparkutils.quality.impl.util.Compare
import com.sparkutils.quality.impl.util.MapUtils.{getKeyIndex, growMap, replaceEntry}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData, MapData}
import org.apache.spark.sql.types.{DataType, LongType, StructType}

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

  def mergeStats(from: InternalRow, into: InternalRow): Unit =
    for(i <- 1 to 8) {
      into.update(i, into.getLong(i) + from.getLong(i))
    }

}


case class StatsRowOps[T](/* debug info only */ level: String, statsMapOffset: Int, inputRowResult: Any => T,
                       statsDefaultNestedType: InternalRow, statsNestedType: StructType,
                       statsBuildWhenNewMap: (InternalRow, MapData) => InternalRow,
                       nextInputLevels: Any => MapData, nextInputRowSize: Int, nextStatMap: InternalRow => MapData,
                       nextStatType: StructType, nextStatsBuildWhenNewMap: (InternalRow, MapData) => InternalRow,
                       processStats: (T, InternalRow) => Unit)

object StatsRowOps {

  lazy val defaultGroupStats = Literal.default(rsType).value.asInstanceOf[InternalRow]
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
  val noOpMapF = (set: InternalRow, m: MapData) => set

  val emptyAr = new GenericArrayData(Array.ofDim[Any](0))
  val emptyMap = new ArrayBasedMapData(emptyAr, emptyAr)

  val processConfig = Seq(
    StatsRowOps(level = "group_to_suite", statsMapOffset = 0,
      inputRowResult = _.asInstanceOf[InternalRow].getInt(1), statsDefaultNestedType = defaultSuiteStats,
      statsNestedType = rsType, statsBuildWhenNewMap = (curGroup, m) => InternalRow(m, curGroup.getLong(1)),
      nextInputLevels = row => row.asInstanceOf[InternalRow].getMap(2), nextInputRowSize = 2,
      nextStatMap = _.getMap(10), nextStatType = setType, nextStatsBuildWhenNewMap = suiteMapF,
      processStats = (r: Int, row: InternalRow) => {
        updateStats(r, row)
        row.update(9, row.getLong(9) + 1L)
      }),
    StatsRowOps(level = "suite_to_set", statsMapOffset = 10,
      inputRowResult = _.asInstanceOf[InternalRow].getInt(0), statsDefaultNestedType = defaultSetStats,
      statsNestedType = setType, statsBuildWhenNewMap =  noOpMapF,
      nextInputLevels = row => row.asInstanceOf[InternalRow].getMap(1), nextInputRowSize = -1,
      nextStatMap = _.getMap(9), nextStatType = rType, nextStatsBuildWhenNewMap = setMapF,
      processStats = updateStats),
    // most of the bit below are ignored for a rule set level result
    StatsRowOps(level = "set_to_rule", statsMapOffset = 9,
      inputRowResult = _.asInstanceOf[Int], statsDefaultNestedType = defaultRuleStats,
      statsNestedType = rType, statsBuildWhenNewMap = noOpMapF,
      nextInputLevels = _ => emptyMap, nextInputRowSize = -1,
      nextStatMap = _ => emptyMap, nextStatType = rType, nextStatsBuildWhenNewMap = (row, _) => row,
      processStats = updateStats)
  )

  val combineConfig = Seq(
    StatsRowOps(level = "group_to_group", statsMapOffset = 0,
      inputRowResult = _.asInstanceOf[InternalRow],
      statsDefaultNestedType = defaultGroupStats, // never used as it's always present
      statsNestedType = rgType,
      statsBuildWhenNewMap = (_, m) => m.valueArray().getStruct(0, 2), // the actual data
      nextInputLevels = row => row.asInstanceOf[InternalRow].getMap(0), nextInputRowSize = 11,
      nextStatMap = _.getMap(0), nextStatType = rsType,
      nextStatsBuildWhenNewMap = (cur: InternalRow, m: MapData) => {
        InternalRow(m, cur.getLong(1))
      },
      processStats = (from: InternalRow, into: InternalRow) => {
        // group rowcount
        into.setLong(1, into.getLong(1) + from.getLong(1))
      }),
    StatsRowOps(level = "group_to_suite", statsMapOffset = 0,
      inputRowResult = _.asInstanceOf[InternalRow], statsDefaultNestedType = defaultSuiteStats,
      statsNestedType = rsType, statsBuildWhenNewMap = noOpMapF,
      nextInputLevels = row => row.asInstanceOf[InternalRow].getMap(10), nextInputRowSize = 10,
      nextStatMap = _.getMap(10), nextStatType = setType, nextStatsBuildWhenNewMap = suiteMapF,
      processStats = (from: InternalRow, into: InternalRow) => {
        mergeStats(from, into)
        // RuleSuite rowcount
        into.setLong(9, into.getLong(9) + from.getLong(9))
      }),
    StatsRowOps(level = "suite_to_set", statsMapOffset = 10,
      inputRowResult = _.asInstanceOf[InternalRow], statsDefaultNestedType = defaultSetStats,
      statsNestedType = setType, statsBuildWhenNewMap =  noOpMapF,
      nextInputLevels = row => row.asInstanceOf[InternalRow].getMap(9), nextInputRowSize = 9,
      nextStatMap = _.getMap(9), nextStatType = rType, nextStatsBuildWhenNewMap = setMapF,
      processStats = mergeStats),
    // most of the bit below are ignored for a rule set level result
    StatsRowOps(level = "set_to_rule", statsMapOffset = 9,
      inputRowResult = _.asInstanceOf[InternalRow], statsDefaultNestedType = defaultRuleStats,
      statsNestedType = rType, statsBuildWhenNewMap = noOpMapF,
      nextInputLevels = _ => emptyMap, nextInputRowSize = 9,
      nextStatMap = _ => emptyMap, nextStatType = rType, nextStatsBuildWhenNewMap = (row, _) => row,
      processStats = mergeStats)
  )

  def allMergedIn(base: MapData, newRows: Seq[(InternalRow, Boolean)]): Boolean = {
    val inplace = base.valueArray().asInstanceOf[GenericArrayData].array

    val bN = base.numElements()
    val bK = base.keyArray()

    var notIn = false
    var i = 0
    while( i < newRows.length && !notIn) {
      val id = newRows(i)._1.getLong(0)
      var j = 0
      var in = false
      while( j < bN && !in) {
        if (bK.getLong(j) == id) {
          in = true
          inplace.update(j, newRows(i)._1)
        }
        j += 1
      }
      notIn = !in
      i += 1
    }
    !notIn
  }

  /**
   * Traverse a pair of maps updating the curGroup from the row
   * @param curGroup the target
   * @param row source information
   * @return RuleSuiteGroupStatistics
   */
  def processWithConfig[T](id: Long, curGroup: InternalRow, row: InternalRow, stats: Seq[StatsRowOps[T]]) = {

    def process(id: Long, row: Any, cur: InternalRow, stats: Seq[StatsRowOps[T]], depth: Int): (InternalRow, Boolean) = {
      val c = stats(depth)
      import c._

      val m = cur.getMap(statsMapOffset)

      val rs_i = getKeyIndex(m, (ka, i) => m.keyArray().getLong(i) == id)
      var createdNewMap = false
      val rs =
        if (rs_i > -1)
          m.valueArray().getStruct(rs_i, statsNestedType.length)
        else {
          val r = statsDefaultNestedType.copy()
          r.setLong(0, id) // always the first field in the stats

          createdNewMap = true
          r
        }

      processStats(inputRowResult(row), rs)

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
              , rs, stats, depth + 1) // this should be the correct row, so we should be inside already...

      // if any new rows were added below then we'll have to re-create all above
      val createdSubMap = newRows.exists(_._2)
      def newRow: InternalRow = {
        // if rs is GenericArrayData map based we may be able to in-place swap entries, scanning will be quicker than map changes
        // if there are no new entries (there may be less)
        if (nextSM.numElements() >= resultRows.numElements() && nextSM.numElements() > 0 &&
          nextSM.valueArray().isInstanceOf[GenericArrayData] && allMergedIn(nextSM, newRows)) {
          rs
        } else {
          // newRows may be updated or added, but there are also possible left-overs
          val newMap = mutable.Map.empty[Long, InternalRow] // TODO perhaps an array is quicker for smaller  volumes
          for {
            i <- 0 until nextSM.numElements()
          } {
            newMap.put(nextSM.keyArray().getLong(i), nextSM.valueArray().getStruct(i, nextStatType.length))
          }
          for {
            i <- newRows.indices
          } {
            newMap.put(newRows(i)._1.getLong(0), newRows(i)._1)
          }
          val keys = new GenericArrayData(newMap.keys.toArray)
          val values = new GenericArrayData(newMap.values.toArray)
          nextStatsBuildWhenNewMap(rs, new ArrayBasedMapData(keys, values))
        }
      }

      (if (depth == stats.length || !(createdNewMap || createdSubMap))
        rs
      else
        newRow,
        createdNewMap || createdSubMap)
    }

    val res = process(id, row, curGroup, stats, 0)
    if (res._2) {
      // some form of map change needed below, so it must cascade up
      val head = stats.head
      import head._
      // either we need to re-integrate it or it was brand new
      val m = curGroup.getMap(statsMapOffset)
      var rs_i = getKeyIndex(m, (ka, i) => m.keyArray().getLong(i) == id)
      val nm =
        if (rs_i > -1) {
          // it needs to be replaced if the arrays are not generic
          replaceEntry(m, LongType, statsNestedType, rs_i, (id, res._1))
        } else {
          // it was never there
          val nm = growMap(m, LongType, statsNestedType,
            (id, res._1))
          rs_i = m.numElements()
          nm
        }

      statsBuildWhenNewMap(curGroup, nm)
    } else {
      // no structural change in underlying row, all in place
      curGroup
    }

  }

  /**
   * Processes a result from a row into the current partitions grouped stats
   * @param curGroup the buffered RuleSuiteGroupStatistics
   * @param row RuleSuiteResult from a row
   * @return RuleSuiteGroupStatistics
   */
  def processResult(curGroup: InternalRow, row: InternalRow) = {
    curGroup.update(1, curGroup.getLong(1) + 1L)

    processWithConfig(row.getLong(0), curGroup, row, processConfig)
  }

  /**
   * Combines group stats from partitions / final to driver
   * @param into RuleSuiteGroupStatistics
   * @param from RuleSuiteGroupStatistics
   * @return RuleSuiteGroupStatistics
   */
  def combineResult(into: InternalRow, from: InternalRow) = {
    // simple wrapper to provide a map and re-use the code
    val wrapped = InternalRow(new ArrayBasedMapData(new GenericArrayData(Array(1L)), new GenericArrayData(Array(into))))
    combineResultWrapped(wrapped, from)
  }

  /**
   * Combines group stats from partitions / final to driver
   * @param into RuleSuiteGroupStatistics
   * @param from RuleSuiteGroupStatistics
   * @return RuleSuiteGroupStatistics
   */
  def combineResultWrapped(wrapped: InternalRow, from: InternalRow) = {
    // either wrapped if it has no structural changes or
    val res = processWithConfig(1L, wrapped, from, combineConfig)
    if (res.numFields == 1)
      // no structural change
      res.getMap(0).valueArray().getStruct(0, rgType.length)
    else
      // had a structural change and is re-built
      res
  }
}

case class ProcessStatistics(children: Seq[Expression]) extends Expression with CodegenFallback {

  lazy val Seq(grp, col) = children

  override def eval(input: InternalRow): Any = {
    val curGroup = grp.eval(input).asInstanceOf[InternalRow]
    val rcol = col.eval(input)

    if (rcol == null)
      curGroup
    else
      processResult(curGroup,
        rcol.asInstanceOf[InternalRow])
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)

  override def dataType: DataType = rgType

  override def nullable: Boolean = false
}

case class MergeStatistics(children: Seq[Expression]) extends Expression with CodegenFallback {

  lazy val Seq(left, right) = children

  val ar: Array[InternalRow] = Array.ofDim(1)
  lazy val wrapped = InternalRow(new ArrayBasedMapData(new GenericArrayData(Array(1L)), new GenericArrayData(ar)))

  override def eval(input: InternalRow): Any = {
    ar.update(0, left.eval(input).asInstanceOf[InternalRow])

    StatsRowOps.combineResultWrapped(
      wrapped,
      right.eval(input).asInstanceOf[InternalRow]
    )
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)

  override def dataType: DataType = rgType

  override def nullable: Boolean = false
}

case class StatisticsDeclarative(children: Seq[Expression]) extends DeclarativeAggregate {

  lazy val Seq(col, groupSer) = children

  lazy val empty = groupSer.eval(InternalRow(RuleSuiteGroupStatistics()))

  override def checkInputDataTypes(): TypeCheckResult =
    if (Compare.equalsIgnoreCaseAndNullability(col.dataType, com.sparkutils.quality.impl.types.ruleSuiteResultType))
      TypeCheckResult.TypeCheckSuccess
    else
      TypeCheckResult.TypeCheckFailure(s"Statistics accepts RuleSuiteResult columns but was provided ${col.dataType}")

  val sumDataType = rgType
  lazy val sum = AttributeReference("sum", sumDataType)()

  override val initialValues: Seq[Expression] = Seq(new Literal(empty, sumDataType))

  override val updateExpressions: Seq[Expression] = Seq(
    ProcessStatistics(Seq(sum, col))
  )

  override val mergeExpressions: Seq[Expression] = Seq(
    MergeStatistics(Seq(sum.left, sum.right))
  )

  override val evaluateExpression: Expression = sum

  override def aggBufferAttributes: Seq[AttributeReference] = Seq(sum)

  override def nullable: Boolean = false

  override def dataType: DataType = sumDataType

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)
}
