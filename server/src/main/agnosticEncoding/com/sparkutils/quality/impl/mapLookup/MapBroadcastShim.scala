package com.sparkutils.quality.impl.mapLookup

import com.sparkutils.quality.impl.extension.QualityMapConstants.QUALITY_MAP_BROADCAST_ALL_CHILDREN
import com.sparkutils.quality.impl.mapLookup.MapTypes.MapLookups
import com.sparkutils.testing.ConnectWhenForced.someOrForcedConnect
import org.apache.spark.sql.SparkSession

protected[quality] object MapBroadcastShim {

  /**
   * Broadcast lookup functions, suitable for agnostic encoding, a no-op on classic
   * @param mapLookups
   * @return
   */
  def broadcast(mapLookups: MapLookups): Option[Unit] = someOrForcedConnect {
    // force the load
    SparkSession.active.sql(s"select map_lookup('$QUALITY_MAP_BROADCAST_ALL_CHILDREN',null,$mapLookups)").head()
  }
}
