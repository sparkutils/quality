package com.sparkutils.quality.impl.util

import scala.collection._

// DELME
// borrowed from https://docs.scala-lang.org/overviews/core/architecture-of-scala-collections.html
class PrefixTrie[T] {

  var suffixes: immutable.Map[Char, PrefixTrie[T]] = immutable.Map.empty
  var value: Option[T] = None

  def get(s: String): Option[T] =
    if (s.isEmpty) value
    else suffixes get (s(0)) flatMap (_.get(s substring 1))

  def withPrefix(s: String): PrefixTrie[T] =
    if (s.isEmpty) this
    else {
      val leading = s(0)
      suffixes get leading match {
        case None =>
          suffixes = suffixes + (leading -> empty)
        case _ =>
      }
      suffixes(leading) withPrefix (s substring 1)
    }

  def update(s: String, elem: T): Unit =
    withPrefix(s).value = Some(elem)

  def remove(s: String): Option[T] =
    if (s.isEmpty) { val prev = value; value = None; prev }
    else suffixes get (s(0)) flatMap (_.remove(s substring 1))

  def iterator: Iterator[(String, T)] =
    (for (v <- value.iterator) yield ("", v)) ++
      (for ((chr, m) <- suffixes.iterator;
            (s, v) <- m.iterator) yield (chr +: s, v))

  def empty = new PrefixTrie[T]

  // cannot use override
  def addOne(kv: (String, T)): PrefixTrie.this.type = { update(kv._1, kv._2); this }

  def subtractOne(s: String): PrefixTrie.this.type = { remove(s); this }
}