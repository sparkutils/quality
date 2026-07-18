package com.sparkutils.quality.impl.mapLookup

import com.sparkutils.quality.impl.mapLookup.MapTypes.MapLookups

protected[quality] object MapBroadcastShim {

  /**
   * Broadcast lookup functions, suitable for agnostic encoding, a no-op on classic
   * @param mapLookups
   * @return
   */
  def broadcast(mapLookups: MapLookups): Option[Unit] = None
}
