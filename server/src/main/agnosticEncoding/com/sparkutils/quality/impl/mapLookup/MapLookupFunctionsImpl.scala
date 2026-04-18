package com.sparkutils.quality.impl.mapLookup

import com.sparkutils.quality.QualityException.qualityException
import com.sparkutils.quality.MapLookups
import com.sparkutils.quality.impl.RuleRegistrationFunctions.registerWithChecks
import org.apache.spark.sql.catalyst.expressions.{Expression, IsNotNull, Literal, VariableReference}
import com.sparkutils.quality.impl.RuleRegistrationFunctions.{getString, literalsNeeded}
import com.sparkutils.quality.impl.extension.QualityMapConstants.{QUALITY_MAP_BROADCAST, QUALITY_MAP_BROADCAST_ALL_CHILDREN}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.types.{DataType, MapType, StringType, StructField, StructType}

import javax.annotation.concurrent.GuardedBy
import scala.collection.mutable
import scala.ref.WeakReference

protected[quality] case class WeakMapEntry(sourceRow: InternalRow,
                                           broadcast: Broadcast[MapData], valueType: DataType) {
  // check if there is still a reference and that it's to the same underlying row
  def exists(in: InternalRow): Boolean =
    (sourceRow eq in)

}

object MapLookupFunctionsImpl {

  protected[quality] def refName(v: VariableReference, mapId: String) =
    (v.identifier.name + "_~_" + mapId).intern()

  protected[quality] def refName(v: String, mapId: String) =
    (v.toLowerCase + "_~_" + mapId).intern()

  /* reference equality only, the actual map data is used in case of re-"set"ting.
    the session and the name lookup are needed to allow internalrows to be used for more than one map
   */
  @GuardedBy("this")
  private val hm = new mutable.HashMap[String, mutable.WeakHashMap[SparkSession, WeakMapEntry]]()

  protected[quality] def registerMapLookupsForAgnostic(registerFunction: (String, Seq[Expression] => Expression) => Unit): Unit = {
    def register(name: String, argsf: Seq[Expression] => Expression, paramNumbers: Set[Int] = Set.empty, minimum: Int = -1) =
      registerWithChecks(registerFunction, name, argsf, paramNumbers, minimum)

    val f = (exps: Seq[Expression]) => {
      val mapId = getString(exps(0))

      // use the VariableReference directly rather than unpack it to the literal - ResolveExecuteImmediate does the unpacking to Literal for us
      exps(2) match {
        case v:VariableReference if v.dataType.isInstanceOf[StructType] && mapId != QUALITY_MAP_BROADCAST_ALL_CHILDREN =>
          // the variable is used directly, probably sql
          val st = v.dataType.asInstanceOf[StructType]
          val col = st.fields.zipWithIndex.find(_._1.name == mapId).getOrElse(
            qualityException(s"Quality map_lookup expression called with map name $mapId doesn't exist in struct with type: ${st.sql}")
          )

          val (expr, typ) = registerMap(v, st, col)

          MapLookupExpression(mapId, exps(1), expr, typ)

        case v:VariableReference if v.dataType.isInstanceOf[StructType] && mapId == QUALITY_MAP_BROADCAST_ALL_CHILDREN =>
          val st = v.dataType.asInstanceOf[StructType]
          val cols = st.fields.zipWithIndex
          //
          cols.foreach {
            col =>
              registerMap(v, st, col, false)
          }
          Literal("Loaded")

        case literal: Literal if literal.dataType.isInstanceOf[StringType] =>
          val lookup = getString(literal)

          val nameMap = hm.getOrElse(refName(lookup, mapId),
            qualityException(s"Quality map_lookup expression called with map name $mapId and lookup $lookup doesn't have a name entry, please call use sql(\"$QUALITY_MAP_BROADCAST $lookup\") before attempting lookup")
          )

          val entry =
            nameMap.getOrElse(SparkSession.active,
              qualityException(s"Quality map_lookup expression called with map name $mapId and lookup $lookup doesn't have an entry in the session map, please call use sql(\"$QUALITY_MAP_BROADCAST $lookup\") before attempting lookup")
            )

          MapLookupExpression(mapId, exps(1), entry.broadcast, entry.valueType)

        case _ => literalsNeeded(2, "Variable with StructType or String")
      }
    }
    register("map_lookup", f, Set(3))

    register("map_contains", s => IsNotNull(f(s)), Set(3))
  }

  private def registerMap(v: VariableReference, st: StructType, col: (StructField, Int), extract: Boolean = true) = {
    col match {
      case (f, i) =>
        val mapId = f.name
        if (!f.dataType.isInstanceOf[MapType]) {
          if (extract)
            qualityException(s"Quality map_lookup expression called with map name $mapId doesn't have map type, instead it has: ${f.dataType.sql}")
          else
            null
        } else {

          val name = refName(v, mapId)

          def newNameMap(name: String) = {
            val m = new mutable.WeakHashMap[SparkSession, WeakMapEntry]()
            hm.put(name, m)
            m
          }

          val nameMap = hm.get(name).fold(newNameMap(name))(identity)

          val dt = f.dataType.asInstanceOf[MapType]
          val r = v.eval().asInstanceOf[InternalRow]

          def createEntry: Broadcast[MapData] = {
            // copy needed to keep the map isolated and serializable
            val m = r.getMap(i).copy()

            val b = SparkSession.active.sparkContext.broadcast(m)
            nameMap.put(SparkSession.active, WeakMapEntry(r, b, dt.valueType))
            b
          }

          val b =
            nameMap.get(SparkSession.active).fold(createEntry) {
              cur =>
                if (cur.exists(r))
                  cur.broadcast
                else
                  // it has been updated as the internal row is no longer the same
                  createEntry
            }

          // it's a map ..
          (b, dt.valueType)
        }
    }
  }

  /**
   * No-op on 0.2.0 4.0
   *
   * @param mapLookups
   * @return
   */
  def registerMapLookupsAndFunction(mapLookups: MapLookups): Unit = {
  }

}
