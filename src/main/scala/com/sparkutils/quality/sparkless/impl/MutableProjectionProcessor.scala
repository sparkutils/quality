package com.sparkutils.quality.sparkless.impl

import com.sparkutils.quality.impl.extension.FunNRewrite
import com.sparkutils.quality.impl.util.EmbeddedTypeCorrection
import com.sparkutils.quality.sparkless.impl.Processors.{NO_QUERY_PLANS, isCopyNeeded}
import com.sparkutils.quality.{QualityException, enableOptimizations}
import com.sparkutils.quality.sparkless.{Processor, ProcessorFactory}
import org.apache.spark.sql.{DataFrame, Encoder, QualitySparkUtils, ShimUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{MutableProjection, PlanExpression}
import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
import org.apache.spark.sql.types.ObjectType

import scala.language.higherKinds

object MutableProjectionProcessor {

  /**
   * Generic processor for encoders over a dataframe transformation
   * @param dataFrameFunction
   * @param compile
   * @tparam I
   * @tparam O
   * @return
   */
  def processFactory[I: Encoder, O: Encoder](dataFrameFunction: DataFrame => DataFrame, embeddedTypeCorrection: EmbeddedTypeCorrection, compile: Boolean = true,
                                             extraProjection: DataFrame => DataFrame = identity, enableQualityOptimisations: Boolean = true): ProcessorFactory[I, O] = {
    if (enableQualityOptimisations) {
      enableOptimizations(Seq(FunNRewrite, ConstantFolding))
    }

    val iEnc = implicitly[Encoder[I]]
    val exprFrom = ShimUtils.expressionEncoder(iEnc).resolveAndBind().serializer

    val (exprs, exprTo) = QualitySparkUtils.resolveExpressions[I, O](iEnc, embeddedTypeCorrection, df => {
      dataFrameFunction(extraProjection(df))
    })

    if (exprs.exists(_.collect {
      case s: PlanExpression[_] => s
      }.nonEmpty)) {
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
              ShimUtils.copyStateful(exprs)
            else
              exprs

          val processor = QualitySparkUtils.rowProcessor(exprsToUse, compile).asInstanceOf[MutableProjection]

          override def apply(i: I): O = {
            val ti = enc(InternalRow(i))
            val r = processor(ti)
            dec(r).get(0, ObjectType(classOf[Any])).asInstanceOf[O]
          }

          /**
           * Sets a partition value for this Process, processes may treat this as a creation of new state
           *
           * @param partition
           */
          override def setPartition(partition: Int): Unit =
            processor.initialize(partition)

          // $COVERAGE-OFF$
          override def close(): Unit = {}
          // $COVERAGE-ON$
        }
    }

  }

}
