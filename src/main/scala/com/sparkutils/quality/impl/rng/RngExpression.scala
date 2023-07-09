package com.sparkutils.quality.impl.rng

import com.sparkutils.quality.impl.StatefulLike
import org.apache.commons.rng.simple.RandomSource
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionWithRandomSeed, LeafExpression, Literal, Rand}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{BinaryType, DataType, LongType, StructField, StructType}

object RandomBytes {
  /**
   * Creates a random number generator using a given commons-rng source
   *
   * @param randomSource commons-rng random source
   * @param numBytes the number of bytes to produce in the array, defaulting to 16
   * @param seed the seed to use / mixin
   * @return a column with the appropriate rng defined
   */
  def apply(randomSource: RandomSource, numBytes: Int = 16, seed: Long = 0): Column =
    new Column( apply(numBytes, randomSource, seed) )

  def apply(numBytes: Int, randomSource: RandomSource, seed: Long): Expression =
    if (randomSource.isJumpable)
      RandBytesWithJump(seed, numBytes, randomSource)
    else
      RandBytesNonJump(seed, numBytes, randomSource)
}

/**
 * Creates a Jumpable random number generator
 * @param definedSeed starting / mixing seed for all new rng instances
 * @param numBytes length of the binary array to fill in bytes
 * @param source
 */
case class RandBytesWithJump(definedSeed: Long, numBytes: Int, source: RandomSource) extends RandBytes with Jumpable {
  type ThisType = RandBytesWithJump
  override def freshCopy: ThisType = copy()
}

case class RandBytesNonJump(definedSeed: Long, numBytes: Int, source: RandomSource) extends RandBytes with RngImpl {
  type ThisType = RandBytesNonJump
  override def freshCopy: ThisType = copy()
}

/**
 * Base implementation for random number byte generation with pluggable implementations
 */
abstract class RandBytes extends LeafExpression with StatefulLike
  with ExpressionWithRandomSeed with CodegenFallback with RngImpl {

  type ThisType <: RandBytes

  override def withNewSeed(seed: Long): ThisType = {
    val r = freshCopy()
    r.reSeed(seed)
    r
  }

  override lazy val resolved: Boolean = isNull

  override def nullable: Boolean = false

  override def dataType: DataType = BinaryType

  override protected def initializeInternal(partitionIndex: Int): Unit = {
    // if rand is already set don't redo
    reSeedOrBranch(partitionIndex)
  }

  override protected def evalInternal(input: InternalRow): Any = {
    val res = nextBytes()
    res
  }

  def seedExpression: Expression = Literal(definedSeed, LongType)
}

object RandomLongs {

  val structType = com.sparkutils.quality.impl.longPair.LongPair.structType

  /**
   * Creates a random number generator using a given commons-rng source
   *
   * @param randomSource commons-rng random source
   * @param seed the seed to use / mixin
   * @return a column with the appropriate rng defined
   */
  def apply(randomSource: RandomSource, seed: Long = 0): Column =
    new Column( create(randomSource, seed) )

  def create(randomSource: RandomSource, seed: Long = 0): Expression =
    if (randomSource.isJumpable)
      RandLongsWithJump(seed, randomSource)
    else
      RandLongsNonJump(seed, randomSource)
}

/**
 * Creates a Jumpable random number generator
 * @param definedSeed starting / mixing seed for all new rng instances
 * @param source
 */
case class RandLongsWithJump(definedSeed: Long, source: RandomSource) extends RandLongs with Jumpable {
  type ThisType = RandLongsWithJump
  override def freshCopy: ThisType = copy()
}

case class RandLongsNonJump(definedSeed: Long, source: RandomSource) extends RandLongs with RngImpl {
  type ThisType = RandLongsNonJump
  override def freshCopy: ThisType = copy()
}

/**
 * Base implementation for random number two long (128 bit) generation with pluggable implementations
 */
abstract class RandLongs extends LeafExpression with StatefulLike
  with ExpressionWithRandomSeed with CodegenFallback with RngImpl {

  type ThisType <: RandLongs

  override def withNewSeed(seed: Long): ThisType = {
    val r = freshCopy()
    r.reSeed(seed)
    r
  }

  override lazy val resolved: Boolean = isNull

  override def nullable: Boolean = false

  override def dataType: DataType = RandomLongs.structType

  override protected def initializeInternal(partitionIndex: Int): Unit = {
    // if rand is already set don't redo
    reSeedOrBranch(partitionIndex)
  }

  override protected def evalInternal(input: InternalRow): Any = {
    InternalRow(nextLong(), nextLong())
  }

  def numBytes: Int = 0

  def seedExpression: Expression = Literal(definedSeed, LongType)
}
