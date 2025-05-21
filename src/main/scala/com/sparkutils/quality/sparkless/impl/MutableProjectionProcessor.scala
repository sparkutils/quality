package com.sparkutils.quality.sparkless.impl

import com.sparkutils.quality.impl.extension.FunNRewrite
import com.sparkutils.quality.sparkless.impl.Processors.{NO_QUERY_PLANS, isCopyNeeded}
import com.sparkutils.quality.{QualityException, enableOptimizations, registerQualityFunctions}
import com.sparkutils.quality.sparkless.{Processor, ProcessorFactory}
import org.apache.spark.sql.{DataFrame, Encoder, QualitySparkUtils, ShimUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, MutableProjection, PlanExpression}
import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
import org.apache.spark.sql.types.{ObjectType, StructType}

object MutableProjectionProcessor {

  /**
   * Generic processor for encoders over a dataframe transformation
   * @param dataFrameFunction
   * @param compile
   * @tparam I
   * @tparam O
   * @return
   */
  def processFactory[I: Encoder, O: Encoder](dataFrameFunction: DataFrame => DataFrame, compile: Boolean = true): ProcessorFactory[I, O] = {
    registerQualityFunctions()
    enableOptimizations(Seq(FunNRewrite, ConstantFolding))

    val iEnc = implicitly[Encoder[I]]
    val exprFrom = ShimUtils.expressionEncoder(iEnc).resolveAndBind().serializer
    val exprTo = ShimUtils.expressionEncoder(implicitly[Encoder[O]]).resolveAndBind().deserializer
    val exprs = QualitySparkUtils.resolveExpressions[I](iEnc, dataFrameFunction)

    val hasSubQuery =
      exprs.map(_.collectFirst {
        case s: PlanExpression[_] => true
      }.getOrElse(false))

    if (hasSubQuery.contains(true)) {
      throw new QualityException(NO_QUERY_PLANS)
    }

    val copyNeeded = isCopyNeeded(exprs, compile)

    new ProcessorFactory[I, O] {

      /**
       * Implementations may return pooled instances and, unless otherwise specified by an implementation, each returned
       * instance should be treated as non-thread safe
       *
       * @return
       */
      override def instance: Processor[I, O] =
        new Processor[I, O] {

          val enc = QualitySparkUtils.rowProcessor(exprFrom, compile).asInstanceOf[MutableProjection]

          val dec = QualitySparkUtils.rowProcessor(Seq(exprTo), compile).asInstanceOf[MutableProjection]

          val exprsToUse =
            if (copyNeeded)
              exprs.map(e => e.transformUp{ case t => t.withNewChildren(t.children) })
            else
              exprs

          val resTypeIsStruct = exprsToUse(exprFrom.length).dataType.isInstanceOf[StructType]
          val resType =
            if (resTypeIsStruct)
              exprsToUse(exprFrom.length).dataType.asInstanceOf[StructType]
            else
              null
          val offset = exprFrom.length

          val processor = QualitySparkUtils.rowProcessor(exprsToUse, compile).asInstanceOf[MutableProjection]

          // to feed the resulting enc
          val interim = new GenericInternalRow(Array.ofDim[Any](exprsToUse.length - offset))

          override def apply(i: I): O = {
            val ti = enc(InternalRow(i))
            val r = processor(ti)
            val ri =
              if (interim.numFields == 1 && resTypeIsStruct)
                r.getStruct(exprFrom.length, resType.length)
              else {
                for(i <- 0 until (exprsToUse.length - offset)) {
                  interim.update(i, r.get(offset + i, exprsToUse(offset + i).dataType))
                }
                interim
              }
            dec(ri).get(0, ObjectType(classOf[Any])).asInstanceOf[O]
          }

          /**
           * Sets a partition value for this Process, processes may treat this as a creation of new state
           *
           * @param partition
           */
          override def setPartition(partition: Int): Unit =
            processor.initialize(partition)

          override def close(): Unit = {}
        }
    }

  }

}
