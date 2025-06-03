package com.sparkutils.quality.sparkless.impl

import com.sparkutils.quality.enableOptimizations
import com.sparkutils.quality.impl.{GenerateDecoderOpEncoderProjection, GenerateDecoderOpEncoderVarProjection}
import com.sparkutils.quality.impl.extension.FunNRewrite
import com.sparkutils.quality.sparkless.{Processor, ProcessorFactory}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Encoder, QualitySparkUtils, ShimUtils}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression}
import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

import scala.reflect.ClassTag

/**
 * Represents a row by row process with Input and Output processors with an operation in between
 */
abstract class DecoderOpEncoderProjection[I, O] extends (I => O) {

  def apply(input: I): O

  /**
   * For compiled code this is a call to new with the same context references if there is no stateful expressions
   * Where expressions are stateful with CodegenFallback the entire GenerateDecoderOpEncoderProjection.generate code is called again.
   */
  def newInstance: DecoderOpEncoderProjection[I, O]

  /**
   * Initialize stateless expressions with this partition
   */
  def initialize(partition: Int): Unit
}

object Processors {
  val NO_QUERY_PLANS = "PlanExpressions (e.g. SubQueries) are not allowed in Processors"

  /**
   * Are there any stateful expressions in interpreted mode (or fallback) require fresh copies
   * @param exprs These expressions must be fully resolved, bound and optimised
   * @param compile when false any stateful expression returns true, by default compilation for stateful CodegenFallback is true
   * @return
   */
  def isCopyNeeded(exprs: Seq[Expression], compile: Boolean = true): Boolean =
    exprs.collect {
      case e: CodegenFallback if ShimUtils.isStateful(e)  => e
    }.nonEmpty || ( !compile &&
      exprs.collect {
        case e: Expression if ShimUtils.isStateful(e)  => e
      }.nonEmpty
    )

  /**
   * Arbitrarily high number assuming 2 java fields per input fields and a couple of functions per field we'll
   * hit the 65k class namespace limit.
   */
  val maxVarCompilationInputFields = 12000

  /**
   * Creates a ProcessFactory[I,O], when forceMutable || !compile is true it uses chained MutableProjections,
   * otherwise, and by default, GenerateDecoderOpEncoderProjection.
   *
   * GenerateDecoderOpEncoderProjection is likely far faster on larger rules or higher number of threads for throughput.
   * Unlike Spark compilation caching, which evaluates codegen for an expression tree every time, the codegen takes place
   * once, each instance call simply returns a new class instance with fresh state for a thread.  If using forceVarCompilation,
   * once the number of input fields goes beyond [[maxVarCompilationInputFields]] the code will fallback to INPUT_ROW based processing.
   *
   * The chained MutableProjections generate new class code for each instance.  They will also recreate the entire
   * expression tree if a stateful expression is identified when compile = false
   *
   * @param dataFrameFunction
   * @param compile when false reverts to interpreted mode, when true and forceMutable is true it is recommended to cache the instances
   * @param forceMutable when true it forces MutableProjection's to be used, compiled or otherwise, the default of false is likely far faster
   * @param forceVarCompilation defaulting to false it will, when compile is true and forceMutable is false, use variables rather than INPUT_ROW to generate code.
   *                            Although using true is faster past 10 fields, the compilation approach (as of 0.1.3.1) is experimental
   *                            and only supported on version OSS 3.2.1 and above
   * @param toSize specifies the number of fields required to deserialize and create the [[O]]
   * @tparam I
   * @tparam O
   * @return
   */
  def processFactory[I: Encoder, O: Encoder](dataFrameFunction: DataFrame => DataFrame, toSize: Int, compile: Boolean = true,
      forceMutable: Boolean = false, forceVarCompilation: Boolean = false, extraProjection: DataFrame => DataFrame = identity,
      enableQualityOptimisations: Boolean = true): ProcessorFactory[I, O] = {
    if (forceMutable || !compile)
      MutableProjectionProcessor.processFactory[I, O](dataFrameFunction, toSize, compile, extraProjection,
        enableQualityOptimisations = enableQualityOptimisations)
    else {
      if (enableQualityOptimisations) {
        enableOptimizations(Seq(FunNRewrite, ConstantFolding))
      }

      val iEnc = implicitly[Encoder[I]]
      val exprs = QualitySparkUtils.resolveExpressions[I](iEnc, df => {
        dataFrameFunction(extraProjection(df))
      })

      // the code references to the other fields is already present inside of expressions, works for input_row based,
      // but not wholestage approach, this is performed by all the CodegenSupport execs via attribute lookups
      val exprsToUse = exprs.drop( exprs.length - toSize)

      val allOrdinals =
        exprsToUse.flatMap{
          e => e.collect {
            case b: BoundReference => b.ordinal
          }
        }.distinct.toSet

      val projector =
        if (forceVarCompilation && allOrdinals.size < maxVarCompilationInputFields)
          GenerateDecoderOpEncoderVarProjection.create[I, O](exprs, toSize, allOrdinals)
        else
          GenerateDecoderOpEncoderProjection.generate[I, O](exprs, useSubexprElimination = true, toSize)
      new ProcessorFactory[I, O] {
        override def instance: Processor[I, O] = new Processor[I, O] {
          private val theInstance = projector.newInstance
          override def apply(i: I): O = theInstance(i)
          override def setPartition(partition: Int): Unit = theInstance.initialize(partition)
          override def close(): Unit = {}
        }
      }
    }
  }

}

case class LocalBroadcast[T: ClassTag](_value: T, _id: Long = 0) extends Broadcast[T](_id) {
  override protected def getValue(): T = _value
  override protected def doUnpersist(blocking: Boolean): Unit = ???
  override protected def doDestroy(blocking: Boolean): Unit = ???
}