package com.sparkutils.quality.impl.util

object Optional {
  def toOptional[T](option: Option[T]): java.util.Optional[T] =
    if (option.isEmpty)
      java.util.Optional.empty()
    else
      java.util.Optional.of(option.get)
}


object MapOps {
  implicit class MapOps[K, +V](map: Map[K,V]) {
    // 2.13 only
    def updatedWithF[V1 >: V](key: K)(remappingFunction: Option[V] => Option[V1]): Map[K,V1] = {
      val previousValue = map.get(key)
      remappingFunction(previousValue) match {
        case None            => previousValue.fold(map)(_ => map - key)
        case Some(nextValue) =>
          if (previousValue.exists(_.asInstanceOf[AnyRef] eq nextValue.asInstanceOf[AnyRef])) map
          else map.updated(key, nextValue)
      }
    }
  }
}