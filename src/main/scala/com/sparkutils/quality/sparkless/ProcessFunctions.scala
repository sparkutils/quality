package com.sparkutils.quality.sparkless

import com.sparkutils.quality.impl.extension.FunNRewrite
import com.sparkutils.quality._
import frameless.TypedExpressionEncoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, MutableProjection}
import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
import org.apache.spark.sql.types.{DataType, ObjectType, StructType}
import org.apache.spark.sql.{DataFrame, Encoder, QualitySparkUtils, ShimUtils}

object ProcessFunctions {

  /**
   * processor for DQ rules
   * @param input
   * @tparam I
   * @return
   */
  def dqFactory[I: Encoder](ruleSuite: RuleSuite, compile: Boolean = true): ProcessorFactory[I, RuleSuiteResult] =
    processFactory[I, RuleSuiteResult](addDataQualityF(ruleSuite), compile)(
      implicitly[Encoder[I]], impl.Encoders.ruleSuiteResultExpEnc
    )

  /**
   * processor for DQ rules
   * @param input
   * @tparam I
   * @return
   */
  def dqDetailsFactory[I: Encoder](ruleSuite: RuleSuite, compile: Boolean = true): ProcessorFactory[I, (RuleResult, RuleSuiteResultDetails)] = {
    import com.sparkutils.quality.implicits._
    val tup = TypedExpressionEncoder[(RuleResult, RuleSuiteResultDetails)]
    processFactory[I, (RuleResult, RuleSuiteResultDetails)](addOverallResultsAndDetailsF(ruleSuite), compile)(
      implicitly[Encoder[I]], tup
    )
  }

  /**
   * processor for ruleEngine
   * @param input
   * @tparam I
   * @tparam T result type
   * @return
   */
  def ruleEngineFactory[I: Encoder, T](ruleSuite: RuleSuite, outputType: DataType, compile: Boolean = true,
    debugMode: Boolean = false)(implicit resEnc: Encoder[RuleEngineResult[T]]): ProcessorFactory[I, RuleEngineResult[T]] =
    processFactory[I, RuleEngineResult[T]](ruleEngineWithStructF(ruleSuite, outputType, debugMode = debugMode), compile)(
      implicitly[Encoder[I]], resEnc)

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

    new ProcessorFactory[I, O] {

      val copyAndSetPartition =
        exprs.exists {
          case e: Expression if e.deterministic => true
        }
      /**
       * Implementations may return pooled instances and, unless otherwise specified by an implementation, each returned
       * instance should be treated as non-thread safe
       *
       * @return
       */
      override def instance: Processor[I, O] =
        new Processor[I, O] {

          val dec = QualitySparkUtils.rowProcessor(exprFrom, compile).asInstanceOf[MutableProjection]
          dec.target(new GenericInternalRow(Array.ofDim[Any](exprFrom.length)))

          val enc = QualitySparkUtils.rowProcessor(Seq(exprTo), compile).asInstanceOf[MutableProjection]
          val toSize =
            exprTo.dataType match {
              case s: StructType => s.length
              case _ => 1
            }
          enc.target(new GenericInternalRow(Array.ofDim[Any](toSize)))

          val exprsToUse =
            if (copyAndSetPartition)
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
          processor.target(new GenericInternalRow(Array.ofDim[Any](exprsToUse.length)))

          // to feed the resulting enc
          val interim = new GenericInternalRow(Array.ofDim[Any](exprsToUse.length - offset))

          override def apply(i: I): O = {
            val ti = dec(InternalRow(i))
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
            enc(ri).get(0, ObjectType(classOf[Any])).asInstanceOf[O]
          }

          /**
           * Sets a partition value for this Process, processes may treat this as a creation of new state
           *
           * @param partition
           */
          override def setPartition(partition: Int): Unit = {
            if (copyAndSetPartition)
              processor.initialize(partition)
            else
              ()
          }

          override def close(): Unit = {}
        }
    }

  }

}
