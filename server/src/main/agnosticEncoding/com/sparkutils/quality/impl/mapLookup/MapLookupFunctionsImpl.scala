package com.sparkutils.quality.impl.mapLookup

import com.sparkutils.quality.QualityException.qualityException
import com.sparkutils.quality.MapLookups
import com.sparkutils.quality.impl.RuleRegistrationFunctions.registerWithChecks
import org.apache.spark.sql.catalyst.expressions.{Expression, IsNotNull}
import com.sparkutils.quality.impl.RuleRegistrationFunctions.{getString, literalsNeeded}
import com.sparkutils.shim.expressions.GetStructField3
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.VariableDefinition
import org.apache.spark.sql.catalyst.expressions.VariableReference
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.types.{MapType, StructType}

import java.util.concurrent.ConcurrentHashMap
import javax.annotation.concurrent.GuardedBy
import scala.collection.mutable
import scala.ref.WeakReference

object MapLookupFunctionsImpl {

  // reference equality only, the actual map data is used in case of re-"set"ting
  @GuardedBy("this")
  private val hm = new mutable.WeakHashMap[MapData, Broadcast[MapData]]()

  protected[quality] def registerMapLookupsForAgnostic(registerFunction: (String, Seq[Expression] => Expression) => Unit): Unit = {
    def register(name: String, argsf: Seq[Expression] => Expression, paramNumbers: Set[Int] = Set.empty, minimum: Int = -1) =
      registerWithChecks(registerFunction, name, argsf, paramNumbers, minimum)

    val f = (exps: Seq[Expression]) => {
      val mapId = getString(exps(0))

      // use the VariableReference directly rather than unpack it to the literal - ResolveExecuteImmediate does the unpacking to Literal for us
      val (expr, typ) = exps(2) match {
        case v:VariableReference if v.dataType.isInstanceOf[StructType] =>
          val st = v.dataType.asInstanceOf[StructType]
          val col = st.fields.zipWithIndex.find(_._1.name == mapId)
          col match {
            case Some((f, i)) =>
              if (!f.dataType.isInstanceOf[MapType])
                qualityException(s"Quality map_lookup expression called with map name $mapId doesn't have map type, instead it has: ${f.dataType.sql}")
              else {
                val m = GetStructField3(v, i, Some(f.name)).eval().asInstanceOf[MapData]
                val b =
                  hm.get(m).fold {
                    val b = SparkSession.active.sparkContext.broadcast(m)
                    hm.put(m, b)
                    b
                  }{
                    _
                  }

                // it's a map ..
                (b, f.dataType.asInstanceOf[MapType].valueType)
              }

            case None =>
              qualityException(s"Quality map_lookup expression called with map name $mapId doesn't exist in struct with type: ${st.sql}")
          }

        case _ => literalsNeeded(2, "StructType")
      }
      MapLookupExpression(mapId, exps(1), expr, typ)
    }
    register("map_lookup", f, Set(3))

    register("map_contains", s => IsNotNull(f(s)), Set(3))
  }

  /**
   * No-op on 0.2.0 4.0
   * @param mapLookups
   * @return
   */
  def registerMapLookupsAndFunction(mapLookups: MapLookups): Unit = {
  }

}
