package com.sparkutils.quality.impl.mapLookup

import com.sparkutils.quality.MapLookups
import com.sparkutils.quality.impl.RuleRegistrationFunctions.registerWithChecks
import org.apache.spark.sql.catalyst.expressions.{Expression, IsNotNull}
import org.apache.spark.sql._

object MapLookupFunctionsImpl {

  /**
   *  Noop on classic (pre 0.2.0 4.0)
   */
  protected[quality] def registerMapLookupsForAgnostic(registerFunction: (String, Seq[Expression] => Expression) => Unit): Unit = {}

  /**
   * On classic (pre 0.2.0 4.0) the map_lookup and map_contains functions are bound to this MapLookups instance
   * @param mapLookups
   */
  def registerMapLookupsAndFunction(mapLookups: MapLookups) {
    val funcReg = {
      val f = ShimUtils.registerFunction(SparkSession.getActiveSession.get) _
      ((n: String, builder: scala.collection.immutable.Seq[Expression] => Expression) =>
        f(n, s => builder(s.toIndexedSeq)))
    }

    def register(name: String, argsf: scala.Seq[Expression] => Expression, paramNumbers: Set[Int] = Set.empty, minimum: Int = -1) =
      registerWithChecks(funcReg, name, argsf, paramNumbers, minimum)

    val f = (exps: Seq[Expression]) => MapLookup(exps(0), exps(1), mapLookups)
    register("map_lookup", f, Set(2))

    val sf = (exps: Seq[Expression]) => IsNotNull(  MapLookup(exps(0), exps(1), mapLookups) )
    register("map_contains", sf, Set(2))
  }

}
