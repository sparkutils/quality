package com.sparkutils.quality.sparkless

import com.sparkutils.quality.{LazyRuleSuiteResultDetails, RuleResult}
import com.sparkutils.quality.impl.LazyRuleSuiteResultDetailsImpl

/**
 * Represents a process from I to O that is, by default, non-thread safe and can be closed, suitable for pools and
 * assigned partitions for non-deterministic or stateful processing.
 *
 * @tparam I
 * @tparam O
 */
trait Processor[I, O] extends Function[I, O] with java.util.function.Function[I, O] with AutoCloseable {
  /**
   * Processes an I and returns an O
   * @param i
   * @return
   */
  def apply(i: I): O

  /**
   * Sets a partition value for this Process, processes may treat this as a creation of new state
   * @param partition
   */
  def setPartition(partition: Int): Unit
}

/**
 * Represents a factory for transformations from I to O.  This factory is thread safe but the underlying Processes may
 * not be.
 *
 * @tparam I
 * @tparam O
 */
trait ProcessorFactory[I, O] {
  /**
   * Implementations may return pooled instances and, unless otherwise specified by an implementation, each returned
   * instance should be treated as non-thread safe
   * @return
   */
  def instance: Processor[I, O]
}

/**
 * Simple proxy from an underlying factory
 * @param underlyingFactory
 * @param convert
 * @tparam I
 * @tparam T
 * @tparam O
 */
case class ProcessorFactoryProxy[I, T, O](underlyingFactory: ProcessorFactory[I, T], convert: T => O) extends ProcessorFactory[I, O] {

  override def instance: Processor[I, O] =
    new Processor[I, O] {
      val underlying = underlyingFactory.instance

      override def apply(i: I): O = convert(underlying.apply(i))

      override def setPartition(partition: Int): Unit = underlying.setPartition(partition)

      override def close(): Unit = underlying.close()
    }
}