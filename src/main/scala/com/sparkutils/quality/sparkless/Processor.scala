package com.sparkutils.quality.sparkless

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
   *
   * @param i
   * @return
   */
  def apply(i: I): O

  /**
   * Sets a partition value for this Process, processes may treat this as a creation of new state
   *
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
   *
   * @return
   */
  def instance: Processor[I, O]
}

trait ProcessorFactoryProxyI[I, T, U, V, S] extends ProcessorFactory[U, V] {

  val underlyingFactory: ProcessorFactory[I, T]

  val stateConstructor: () => S

  protected def theApply(i: U, state: S, underlying: Processor[I, T]): V

  override def instance: Processor[U, V] =
    new Processor[U, V] {

      private val underlying: Processor[I, T] = underlyingFactory.instance

      private val state: S = stateConstructor()

      override def apply(i: U): V = theApply(i, state, underlying)

      override def setPartition(partition: Int): Unit = underlying.setPartition(partition)

      override def close(): Unit = underlying.close()
    }
}

/**
 * @param underlyingFactory
 * @param stateConstructor
 * @param convert
 * @tparam I
 * @tparam T
 * @tparam O
 * @tparam S
 */
class ProcessorFactoryProxyWithState[I, T, O, S](val underlyingFactory: ProcessorFactory[I, T],
                                                 val stateConstructor: () => S, convert: (T, S) => O)
  extends ProcessorFactoryProxyI[I, T, I, O, S] {
  override protected def theApply(i: I, s: S, underlying: Processor[I, T]): O = convert(underlying.apply(i), s)
}

class ProcessorFactoryProxy[I, T, O](underlyingFactory: ProcessorFactory[I, T], convert: T => O)
  extends ProcessorFactoryProxyWithState[I, T, O, Any](underlyingFactory, () => (), (t: T, _) => convert(t))


/**
 * @param underlyingFactory
 * @param stateConstructor
 * @param convert
 * @tparam I
 * @tparam T
 * @tparam O
 * @tparam S
 */
class ProcessorFactoryInputProxyWithState[I, T, O, S](val underlyingFactory: ProcessorFactory[I, T], val stateConstructor: () => S, convert: (S, O) => I)
  extends ProcessorFactoryProxyI[I, T, O, T, S] {
  override protected def theApply(i: O, state: S, underlying: Processor[I, T]): T = underlying.apply(convert(state, i))

}