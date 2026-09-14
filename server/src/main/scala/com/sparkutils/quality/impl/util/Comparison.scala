package com.sparkutils.quality.impl.util

object Comparison {

  /**
   * Forwards to compare, allows for compareToOrdering(ordering) syntax with internal casts
   *
   * @param left
   * @param right
   * @tparam T
   * @return
   */
  def compareToOrdering[T](ordering: Ordering[T])(left: Any, right: Any): Int =
    if (left == null && right != null)
      -100
    else if (right == null && left != null)
      100
    else
      ordering.compare(left.asInstanceOf[T], right.asInstanceOf[T])

}
