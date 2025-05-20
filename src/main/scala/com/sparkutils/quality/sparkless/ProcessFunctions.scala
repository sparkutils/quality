package com.sparkutils.quality.sparkless

import com.sparkutils.quality.impl.extension.FunNRewrite
import com.sparkutils.quality._
import com.sparkutils.quality.impl.util.Encoding.fromNormalEncoder
import frameless.{TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, MutableProjection, ScalarSubquery, SubqueryExpression}
import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
import org.apache.spark.sql.types.{DataType, ObjectType, StructType}
import org.apache.spark.sql.{Column, DataFrame, Encoder, QualitySparkUtils, ShimUtils}

import scala.reflect.ClassTag

object ProcessFunctions {

  /**
   * processor for DQ rules
   * @param input
   * @tparam I
   * @return
   */
  def dqFactory[I: Encoder](ruleSuite: RuleSuite, compile: Boolean = true, compileEvals: Boolean = false,
                            forceRunnerEval: Boolean = false): ProcessorFactory[I, RuleSuiteResult] =
    processFactory[I, RuleSuiteResult](addDataQualityF(ruleSuite, compileEvals = compileEvals,
      forceRunnerEval = forceRunnerEval), compile)(
      implicitly[Encoder[I]], impl.Encoders.ruleSuiteResultExpEnc
    )

  /**
   * processor for DQ rules
   * @param input
   * @tparam I
   * @return
   */
  def dqDetailsFactory[I: Encoder](ruleSuite: RuleSuite, compile: Boolean = true, compileEvals: Boolean = false,
                                   forceRunnerEval: Boolean = false): ProcessorFactory[I, (RuleResult, RuleSuiteResultDetails)] = {
    import com.sparkutils.quality.implicits._
    val tup = TypedExpressionEncoder[(RuleResult, RuleSuiteResultDetails)]
    processFactory[I, (RuleResult, RuleSuiteResultDetails)](addOverallResultsAndDetailsF(ruleSuite,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval), compile)(
      implicitly[Encoder[I]], tup
    )
  }

  /**
   * processor for ruleEngine with encoding over the nested T in RuleEngineResult[T].
   * *Note* you must use AgnosticEncoders in 3.4+ in order to be able to support Java collections/generics,
   * reflection via .bean is not sufficient for java generics.
   * @param input
   * @tparam I the input type
   * @tparam T the result type of the rule engine
   * @return
   */
  def ruleEngineFactoryT[I: Encoder, T: Encoder](ruleSuite: RuleSuite, outputType: DataType, compile: Boolean = true,
      debugMode: Boolean = false, compileEvals: Boolean = false,
      forceRunnerEval: Boolean = false, forceTriggerEval: Boolean = false): ProcessorFactory[I, RuleEngineResult[T]] = {
    import com.sparkutils.quality.implicits._
    implicit val ttyped: TypedEncoder[T] = fromNormalEncoder[T](outputType)
    implicit val enc = TypedExpressionEncoder[RuleEngineResult[T]]
    ruleEngineFactory[I, T](ruleSuite, outputType, compile = compile, debugMode = debugMode,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval, forceTriggerEval = forceTriggerEval)
  }

  /**
   * processor for ruleEngine
   * @param input
   * @tparam I
   * @tparam T result type
   * @return
   */
  def ruleEngineFactory[I: Encoder, T](ruleSuite: RuleSuite, outputType: DataType, compile: Boolean = true,
      debugMode: Boolean = false, compileEvals: Boolean = false,
      forceRunnerEval: Boolean = false, forceTriggerEval: Boolean = false)(implicit resEnc: Encoder[RuleEngineResult[T]]):
        ProcessorFactory[I, RuleEngineResult[T]] =
    processFactory[I, RuleEngineResult[T]](ruleEngineWithStructF(ruleSuite, outputType, debugMode = debugMode,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval, forceTriggerEval = forceTriggerEval), compile)(
      implicitly[Encoder[I]], resEnc)


  /**
   * processor for ruleFolder with encoding over the nested T in RuleFolderResult[T].
   * *Note* you must use AgnosticEncoders in 3.4+ in order to be able to support Java collections/generics,
   * reflection via .bean is not sufficient for java generics.
   * @param input
   * @tparam I the input type
   * @tparam T the result type of the rule engine
   * @return
   */
  def ruleFolderFactoryT[I: Encoder, T: Encoder](ruleSuite: RuleSuite, outputType: StructType, compile: Boolean = true,
      debugMode: Boolean = false, compileEvals: Boolean = false,
      forceRunnerEval: Boolean = false, forceTriggerEval: Boolean = false): ProcessorFactory[I, RuleFolderResult[T]] = {
    import com.sparkutils.quality.implicits._
    implicit val ttyped: TypedEncoder[T] = fromNormalEncoder[T](outputType)
    implicit val enc = TypedExpressionEncoder[RuleFolderResult[T]]
    ruleFolderFactory[I, T](ruleSuite, outputType, compile = compile, debugMode = debugMode,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval, forceTriggerEval = forceTriggerEval)
  }

  /**
   * processor for ruleFolder
   * @param input
   * @tparam I
   * @tparam T result type
   * @return
   */
  def ruleFolderFactory[I: Encoder, T](ruleSuite: RuleSuite, outputType: StructType, compile: Boolean = true,
      debugMode: Boolean = false, compileEvals: Boolean = false,
      forceRunnerEval: Boolean = false, forceTriggerEval: Boolean = false)(implicit resEnc: Encoder[RuleFolderResult[T]]):
      ProcessorFactory[I, RuleFolderResult[T]] =
    processFactory[I, RuleFolderResult[T]](foldAndReplaceFieldsWithStruct(ruleSuite, outputType, debugMode = debugMode,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval, forceTriggerEval = forceTriggerEval), compile)(
      implicitly[Encoder[I]], resEnc)

  /**
   * processor for ruleFolder with encoding over the nested T in RuleFolderResult[T].
   * *Note* you must use AgnosticEncoders in 3.4+ in order to be able to support Java collections/generics,
   * reflection via .bean is not sufficient for java generics.
   * @param input
   * @tparam I the input type
   * @tparam T the result type of the rule engine
   * @return
   */
  def ruleFolderFactoryWithStructStarterT[I: Encoder, T: Encoder](ruleSuite: RuleSuite, fields: Seq[(String, Column)], outputType: StructType, compile: Boolean = true,
                                                 debugMode: Boolean = false, compileEvals: Boolean = false,
                                                 forceRunnerEval: Boolean = false, forceTriggerEval: Boolean = false): ProcessorFactory[I, RuleFolderResult[T]] = {
    import com.sparkutils.quality.implicits._
    implicit val ttyped: TypedEncoder[T] = fromNormalEncoder[T](outputType)
    implicit val enc = TypedExpressionEncoder[RuleFolderResult[T]]
    ruleFolderFactoryWithStructStarter[I, T](ruleSuite, fields, outputType, compile = compile, debugMode = debugMode,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval, forceTriggerEval = forceTriggerEval)
  }

  /**
   * processor for ruleFolder
   * @param input
   * @tparam I
   * @tparam T result type
   * @return
   */
  def ruleFolderFactoryWithStructStarter[I: Encoder, T](ruleSuite: RuleSuite, fields: Seq[(String, Column)], outputType: StructType, compile: Boolean = true,
                                       debugMode: Boolean = false, compileEvals: Boolean = false,
                                       forceRunnerEval: Boolean = false, forceTriggerEval: Boolean = false)(implicit resEnc: Encoder[RuleFolderResult[T]]):
  ProcessorFactory[I, RuleFolderResult[T]] =
    processFactory[I, RuleFolderResult[T]](foldAndReplaceFieldPairsWithStruct(ruleSuite, fields, outputType, debugMode = debugMode,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval, forceTriggerEval = forceTriggerEval), compile)(
      implicitly[Encoder[I]], resEnc)


  /**
   * processor for expressionRunner with encoding over the nested T in GeneralExpressionsResult[T].  You must supply
   * the outputType for T
   * *Note* you must use AgnosticEncoders in 3.4+ in order to be able to support Java collections/generics,
   * reflection via .bean is not sufficient for java generics.
   * @param input
   * @tparam I the input type
   * @tparam T the result type of the rule engine
   * @return
   */
  def expressionRunnerFactoryT[I: Encoder, T: Encoder](ruleSuite: RuleSuite, outputType: DataType,
      compile: Boolean = true, compileEvals: Boolean = false,
      forceRunnerEval: Boolean = false): ProcessorFactory[I, GeneralExpressionsResult[T]] = {
    import com.sparkutils.quality.implicits._
    implicit val ttyped: TypedEncoder[T] = fromNormalEncoder[T](outputType)
    implicit val enc = TypedExpressionEncoder[GeneralExpressionsResult[T]]
    expressionRunnerFactory[I, T](ruleSuite, outputType = outputType, compile = compile,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval)
  }

  /**
   * processor for expressionRunner producing T, the outputType must be specified for the T
   * @param input
   * @tparam I
   * @tparam T result type
   * @return
   */
  def expressionRunnerFactory[I: Encoder, T](ruleSuite: RuleSuite, outputType: DataType,
      compile: Boolean = true, compileEvals: Boolean = false,
      forceRunnerEval: Boolean = false)(implicit resEnc: Encoder[GeneralExpressionsResult[T]]):
      ProcessorFactory[I, GeneralExpressionsResult[T]] =
    processFactory[I, GeneralExpressionsResult[T]](addExpressionRunnerF(ruleSuite, ddlType = outputType.sql,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval), compile)(
      implicitly[Encoder[I]], resEnc)

  /**
   * processor for expressionRunner producing Yaml, resulting in GeneralExpressionsResultNoDDL
   * @param input
   * @tparam I
   * @return
   */
  def expressionYamlRunnerFactory[I: Encoder](ruleSuite: RuleSuite, renderOptions: Map[String, String] = Map.empty,
                                             compile: Boolean = true, compileEvals: Boolean = false,
                                             forceRunnerEval: Boolean = false):
  ProcessorFactory[I, GeneralExpressionsResult[GeneralExpressionResult]] = {
    import com.sparkutils.quality.implicits._
    processFactory[I, GeneralExpressionsResult[GeneralExpressionResult]](addExpressionRunnerF(ruleSuite, renderOptions = renderOptions,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval), compile)(
      implicitly[Encoder[I]], TypedExpressionEncoder[GeneralExpressionsResult[GeneralExpressionResult]])
  }

  /**
   * processor for expressionRunner producing Yaml, resulting in GeneralExpressionsResultNoDDL
   * @param input
   * @tparam I
   * @return
   */
  def expressionYamlNoDDLRunnerFactory[I: Encoder](ruleSuite: RuleSuite, renderOptions: Map[String, String] = Map.empty,
                                              compile: Boolean = true, compileEvals: Boolean = false,
                                              forceRunnerEval: Boolean = false):
  ProcessorFactory[I, GeneralExpressionsResultNoDDL] = {
    import com.sparkutils.quality.implicits._
    processFactory[I, GeneralExpressionsResultNoDDL](addExpressionRunnerF(ruleSuite, renderOptions = renderOptions,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval, stripDDL = true), compile)(
      implicitly[Encoder[I]], generalExpressionsResultNoDDLExpEnc)
  }

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
        case s: SubqueryExpression => true
      }.getOrElse(false))

    if (hasSubQuery.contains(true)) {
      throw new QualityException("SubQuery's are not allowed in Processors")
    }

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
