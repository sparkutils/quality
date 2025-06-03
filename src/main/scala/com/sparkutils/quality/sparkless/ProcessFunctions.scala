package com.sparkutils.quality.sparkless

import com.sparkutils.quality._
import com.sparkutils.quality.impl.{Encoders, LazyRuleSuiteResultDetailsImpl, LazyRuleSuiteResultDetailsProxyImpl, LazyRuleSuiteResultImpl}
import com.sparkutils.quality.impl.util.Encoding.fromNormalEncoder
import com.sparkutils.quality.sparkless.impl.Processors.processFactory
import frameless.{TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Column, DataFrame, Encoder}

import scala.reflect.ClassTag

/**
 * A collection of OSS Spark factory functions for all of Quality's runners provided in two flavours for ruleEngine, folder
 * and expressionRunner.
 *
 * 1. xFactory (requires encoders for both input and the full output type)
 * 2. xFactoryT (requires encoders for the underlying output type wrapped by Quality types)
 *
 * The T variants accept all spark encoders and, on later spark versions, AgnosticEncoders can also be used to specify
 * java collections.  Not using AgnosticEncoders will typically result in cryptic messages about missing types as Java
 * drops it's type information in generated classes.
 *
 * The non-T variants require using Frameless for type derivation with an import of quality.implicits._.
 *
 * No QueryPlan generating expressions (e.g. joins/correlated sub-queries etc.) may be used and their presence will
 * trigger an exception.
 *
 * ProcessorFactory's create new instances on the basis of the compile, forceMutable and forceVarCompilation boolean
 * parameters. Using compile = false will trigger interpreted execution of rules and fresh tree copies for each instance
 * call.  The default compilation approach (forceMutable = false) reduces source code
 * generation overhead for non-stateful expression trees (e.g. no monotonically_incrementing_id or unique_id or rand etc.).
 * With forceMutable set as true a higher cost implementation based on Spark provided MutableProjections will be used,
 * if there are issues in actual processing this option is worth trying.
 *
 * EXPERIMENTAL - forceVarCompilation = true uses a wholeStageCodeGen style approach for subexpressions and removes
 * the use of InternalRow wherever possible, the default of forceVarCompilation = true uses InternalRow based processing.
 * The compilation overhead is higher with VarCompilation, albeit typically paid once, but the runtime performance is
 * the fastest of the options. The compilation approach however is closer to ruleRunner than WholestageCodegen and is
 * therefor deemed supported but experimental.  It is only supported on version 3.2.1 and above.
 *
 * The defaults, including runner specific compilation options, are chosen for general larger volumes of data processing.
 *
 * In some cases the input may need a further projection, e.g. from_avro, the extraProjection function allows for this.
 *
 * By default the Quality optimisations are added, e.g. funRewrite.  These may be disabled vai setting
 * enableQualityOptimisations = false.
 */
object ProcessFunctions {

  /**
   * processor for DQ rules
   * @param input
   * @tparam I
   * @return
   */
  def dqFactory[I: Encoder](ruleSuite: RuleSuite, compile: Boolean = true, compileEvals: Boolean = false,
      forceRunnerEval: Boolean = false, forceMutable: Boolean = false,
      extraProjection: DataFrame => DataFrame = identity, enableQualityOptimisations: Boolean = true,
      forceVarCompilation: Boolean = false): ProcessorFactory[I, RuleSuiteResult] =
    processFactory[I, RuleSuiteResult](addDataQualityF(ruleSuite, compileEvals = compileEvals,
      forceRunnerEval = forceRunnerEval), 1, compile, forceMutable = forceMutable,
      extraProjection = extraProjection, enableQualityOptimisations = enableQualityOptimisations,
      forceVarCompilation = forceVarCompilation)(
      implicitly[Encoder[I]], com.sparkutils.quality.impl.Encoders.ruleSuiteResultExpEnc
    )

  /**
   * processor for DQ rules returning a pair of overall RuleResult and RuleSuiteResultDetails
   * @param input
   * @tparam I
   * @return
   */
  def dqDetailsFactory[I: Encoder](ruleSuite: RuleSuite, compile: Boolean = true, compileEvals: Boolean = false,
      forceRunnerEval: Boolean = false, forceMutable: Boolean = false,
      extraProjection: DataFrame => DataFrame = identity, enableQualityOptimisations: Boolean = true,
      forceVarCompilation: Boolean = false): ProcessorFactory[I, (RuleResult, RuleSuiteResultDetails)] = {
    import com.sparkutils.quality.implicits._
    val tup = TypedExpressionEncoder[(RuleResult, RuleSuiteResultDetails)]
    processFactory[I, (RuleResult, RuleSuiteResultDetails)](addOverallResultsAndDetailsF(ruleSuite,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval), 2, compile, forceMutable = forceMutable,
      extraProjection = extraProjection, enableQualityOptimisations = enableQualityOptimisations,
      forceVarCompilation = forceVarCompilation)(
      implicitly[Encoder[I]], tup
    )
  }

  /**
   * processor for DQ rules returning a pair of overall RuleResult and a LazyRuleSuiteResultDetails
   * @param input
   * @tparam I
   * @return
   */
  def dqLazyDetailsFactory[I: Encoder](ruleSuite: RuleSuite, compile: Boolean = true, compileEvals: Boolean = false,
                                   forceRunnerEval: Boolean = false, forceMutable: Boolean = false,
                                   extraProjection: DataFrame => DataFrame = identity, enableQualityOptimisations: Boolean = true,
                                   forceVarCompilation: Boolean = false, defaultIfPassed: Option[RuleSuiteResultDetails] = None):
          ProcessorFactory[I, (RuleResult, LazyRuleSuiteResultDetails)] = {
    import com.sparkutils.quality.implicits._
    implicit val rowEnc = Encoders.internalRowTypedEnc(Encoders.ruleSuiteResultDetailsTypedEnc.catalystRepr)

    val tup = TypedExpressionEncoder[(RuleResult, InternalRow)]
    val defaultIfPassedProxy = defaultIfPassed.map(LazyRuleSuiteResultDetailsProxyImpl(_))

    val r = processFactory[I, (RuleResult, InternalRow)](addOverallResultsAndDetailsF(ruleSuite,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval), 2, compile, forceMutable = forceMutable,
      extraProjection = extraProjection, enableQualityOptimisations = enableQualityOptimisations,
      forceVarCompilation = forceVarCompilation)(
      implicitly[Encoder[I]], tup
    )

    ProcessorFactoryProxy(r, (p: (RuleResult, InternalRow)) => {
      (p._1,
        if (p._1 == Passed)
          defaultIfPassedProxy.getOrElse[LazyRuleSuiteResultDetails](
            LazyRuleSuiteResultDetailsImpl(p._2)
          ) else
          LazyRuleSuiteResultDetailsImpl(p._2)
      )
    })
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
      forceRunnerEval: Boolean = false, forceTriggerEval: Boolean = false, forceMutable: Boolean = false,
      extraProjection: DataFrame => DataFrame = identity, enableQualityOptimisations: Boolean = true,
      forceVarCompilation: Boolean = false): ProcessorFactory[I, RuleEngineResult[T]] = {
    import com.sparkutils.quality.implicits._
    implicit val ttyped: TypedEncoder[T] = fromNormalEncoder[T](outputType)
    implicit val enc = TypedExpressionEncoder[RuleEngineResult[T]]
    ruleEngineFactory[I, T](ruleSuite, outputType, compile = compile, debugMode = debugMode,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval, forceTriggerEval = forceTriggerEval,
      forceMutable = forceMutable, extraProjection = extraProjection, enableQualityOptimisations = enableQualityOptimisations,
      forceVarCompilation = forceVarCompilation)
  }

  /**
   * processor for ruleEngine with encoding over the nested T in RuleEngineResult[T] with lazy serialisation of the RuleSuiteResult.
   * *Note* you must use AgnosticEncoders in 3.4+ in order to be able to support Java collections/generics,
   * reflection via .bean is not sufficient for java generics.
   * @param input
   * @tparam I the input type
   * @tparam T the result type of the rule engine
   * @return
   */
  def ruleEngineLazyResultFactory[I: Encoder, T: Encoder](ruleSuite: RuleSuite, outputType: DataType, compile: Boolean = true,
                                                 debugMode: Boolean = false, compileEvals: Boolean = false,
                                                 forceRunnerEval: Boolean = false, forceTriggerEval: Boolean = false, forceMutable: Boolean = false,
                                                 extraProjection: DataFrame => DataFrame = identity, enableQualityOptimisations: Boolean = true,
                                                 forceVarCompilation: Boolean = false)(implicit oenc: Encoder[Option[T]]): ProcessorFactory[I, LazyRuleEngineResult[T]] = {
    import com.sparkutils.quality.implicits._
    implicit val rowEnc = Encoders.internalRowTypedEnc(Encoders.ruleSuiteResultTypedEnc.catalystRepr)

    implicit val ttyped: TypedEncoder[T] = fromNormalEncoder[T](outputType)
    implicit val enc = TypedExpressionEncoder[(InternalRow, Option[SalientRule], Option[T])]

    val r = processFactory[I, (InternalRow, Option[SalientRule], Option[T])](ruleEngineWithStructF(ruleSuite, outputType, debugMode = debugMode,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval, forceTriggerEval = forceTriggerEval), 1, compile,
      forceMutable = forceMutable, extraProjection = extraProjection, enableQualityOptimisations = enableQualityOptimisations,
      forceVarCompilation = forceVarCompilation)(
      implicitly[Encoder[I]], enc)

    ProcessorFactoryProxy(r, (p: ((InternalRow, Option[SalientRule], Option[T]))) => {
      LazyRuleEngineResult(LazyRuleSuiteResultImpl(p._1), p._2, p._3)
    })
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
      forceRunnerEval: Boolean = false, forceTriggerEval: Boolean = false, forceMutable: Boolean = false,
      extraProjection: DataFrame => DataFrame = identity, enableQualityOptimisations: Boolean = true,
      forceVarCompilation: Boolean = false)(implicit resEnc: Encoder[RuleEngineResult[T]]):
        ProcessorFactory[I, RuleEngineResult[T]] =
    processFactory[I, RuleEngineResult[T]](ruleEngineWithStructF(ruleSuite, outputType, debugMode = debugMode,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval, forceTriggerEval = forceTriggerEval), 1, compile,
      forceMutable = forceMutable, extraProjection = extraProjection, enableQualityOptimisations = enableQualityOptimisations,
      forceVarCompilation = forceVarCompilation)(
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
      forceRunnerEval: Boolean = false, forceTriggerEval: Boolean = false, forceMutable: Boolean = false,
      extraProjection: DataFrame => DataFrame = identity, enableQualityOptimisations: Boolean = true,
      forceVarCompilation: Boolean = false): ProcessorFactory[I, RuleFolderResult[T]] = {
    import com.sparkutils.quality.implicits._
    implicit val ttyped: TypedEncoder[T] = fromNormalEncoder[T](outputType)
    implicit val enc = TypedExpressionEncoder[RuleFolderResult[T]]
    ruleFolderFactory[I, T](ruleSuite, outputType, compile = compile, debugMode = debugMode,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval, forceTriggerEval = forceTriggerEval,
      forceMutable = forceMutable, extraProjection = extraProjection, enableQualityOptimisations = enableQualityOptimisations,
      forceVarCompilation = forceVarCompilation)
  }

  /**
   * processor for ruleFolder with encoding over the nested T in RuleFolderResult[T], lazy serialising RuleSuiteResults.
   * *Note* you must use AgnosticEncoders in 3.4+ in order to be able to support Java collections/generics,
   * reflection via .bean is not sufficient for java generics.
   * @param input
   * @tparam I the input type
   * @tparam T the result type of the rule engine
   * @return
   */
  def ruleFolderLazyFactory[I: Encoder, T: Encoder](ruleSuite: RuleSuite, outputType: StructType, compile: Boolean = true,
                                                 debugMode: Boolean = false, compileEvals: Boolean = false,
                                                 forceRunnerEval: Boolean = false, forceTriggerEval: Boolean = false, forceMutable: Boolean = false,
                                                 extraProjection: DataFrame => DataFrame = identity, enableQualityOptimisations: Boolean = true,
                                                 forceVarCompilation: Boolean = false): ProcessorFactory[I, LazyRuleFolderResult[T]] = {
    import com.sparkutils.quality.implicits._
    implicit val rowEnc = Encoders.internalRowTypedEnc(Encoders.ruleSuiteResultTypedEnc.catalystRepr)

    implicit val ttyped: TypedEncoder[T] = fromNormalEncoder[T](outputType)
    implicit val enc = TypedExpressionEncoder[(InternalRow, Option[T])]

    val r = processFactory[I, (InternalRow, Option[T])](foldAndReplaceFieldsWithStruct(ruleSuite, outputType, debugMode = debugMode,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval, forceTriggerEval = forceTriggerEval), 1, compile,
      forceMutable = forceMutable, extraProjection = extraProjection, enableQualityOptimisations = enableQualityOptimisations,
      forceVarCompilation = forceVarCompilation)(
      implicitly[Encoder[I]], enc)

    ProcessorFactoryProxy(r, (p: ((InternalRow, Option[T]))) => {
      LazyRuleFolderResult(LazyRuleSuiteResultImpl(p._1), p._2)
    })
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
      forceRunnerEval: Boolean = false, forceTriggerEval: Boolean = false, forceMutable: Boolean = false,
      extraProjection: DataFrame => DataFrame = identity, enableQualityOptimisations: Boolean = true,
      forceVarCompilation: Boolean = false)(implicit resEnc: Encoder[RuleFolderResult[T]]):
      ProcessorFactory[I, RuleFolderResult[T]] =
    processFactory[I, RuleFolderResult[T]](foldAndReplaceFieldsWithStruct(ruleSuite, outputType, debugMode = debugMode,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval, forceTriggerEval = forceTriggerEval), 1, compile,
      forceMutable = forceMutable, extraProjection = extraProjection, enableQualityOptimisations = enableQualityOptimisations,
      forceVarCompilation = forceVarCompilation)(
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
  def ruleFolderFactoryWithStructStarterT[I: Encoder, T: Encoder](ruleSuite: RuleSuite, fields: Seq[(String, Column)],
      outputType: StructType, compile: Boolean = true, debugMode: Boolean = false, compileEvals: Boolean = false,
      forceRunnerEval: Boolean = false, forceTriggerEval: Boolean = false, forceMutable: Boolean = false,
      extraProjection: DataFrame => DataFrame = identity, enableQualityOptimisations: Boolean = true,
      forceVarCompilation: Boolean = false): ProcessorFactory[I, RuleFolderResult[T]] = {
    import com.sparkutils.quality.implicits._
    implicit val ttyped: TypedEncoder[T] = fromNormalEncoder[T](outputType)
    implicit val enc = TypedExpressionEncoder[RuleFolderResult[T]]
    ruleFolderFactoryWithStructStarter[I, T](ruleSuite, fields, outputType, compile = compile, debugMode = debugMode,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval, forceTriggerEval = forceTriggerEval,
      forceMutable = forceMutable, extraProjection = extraProjection, enableQualityOptimisations = enableQualityOptimisations,
      forceVarCompilation = forceVarCompilation)
  }

  /**
   * processor for ruleFolder with encoding over the nested T in RuleFolderResult[T], lazily serialising the RuleSuiteResults
   * *Note* you must use AgnosticEncoders in 3.4+ in order to be able to support Java collections/generics,
   * reflection via .bean is not sufficient for java generics.
   * @param input
   * @tparam I the input type
   * @tparam T the result type of the rule engine
   * @return
   */
  def ruleFolderLazyFactoryWithStructStarter[I: Encoder, T: Encoder](ruleSuite: RuleSuite, fields: Seq[(String, Column)],
                                                                  outputType: StructType, compile: Boolean = true, debugMode: Boolean = false, compileEvals: Boolean = false,
                                                                  forceRunnerEval: Boolean = false, forceTriggerEval: Boolean = false, forceMutable: Boolean = false,
                                                                  extraProjection: DataFrame => DataFrame = identity, enableQualityOptimisations: Boolean = true,
                                                                  forceVarCompilation: Boolean = false): ProcessorFactory[I, LazyRuleFolderResult[T]] = {
    import com.sparkutils.quality.implicits._
    implicit val rowEnc = Encoders.internalRowTypedEnc(Encoders.ruleSuiteResultTypedEnc.catalystRepr)

    implicit val ttyped: TypedEncoder[T] = fromNormalEncoder[T](outputType)
    implicit val enc = TypedExpressionEncoder[(InternalRow, Option[T])]

    val r = processFactory[I, (InternalRow, Option[T])](foldAndReplaceFieldPairsWithStruct(ruleSuite, fields, outputType, debugMode = debugMode,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval, forceTriggerEval = forceTriggerEval), 1, compile,
      forceMutable = forceMutable, extraProjection = extraProjection, enableQualityOptimisations = enableQualityOptimisations,
      forceVarCompilation = forceVarCompilation)(
      implicitly[Encoder[I]], enc)

    ProcessorFactoryProxy(r, (p: ((InternalRow, Option[T]))) => {
      LazyRuleFolderResult(LazyRuleSuiteResultImpl(p._1), p._2)
    })
  }

  /**
   * processor for ruleFolder
   * @param input
   * @tparam I
   * @tparam T result type
   * @return
   */
  def ruleFolderFactoryWithStructStarter[I: Encoder, T](ruleSuite: RuleSuite, fields: Seq[(String, Column)],
      outputType: StructType, compile: Boolean = true, debugMode: Boolean = false, compileEvals: Boolean = false,
      forceRunnerEval: Boolean = false, forceTriggerEval: Boolean = false, forceMutable: Boolean = false,
      extraProjection: DataFrame => DataFrame = identity, enableQualityOptimisations: Boolean = true,
      forceVarCompilation: Boolean = false)(implicit resEnc: Encoder[RuleFolderResult[T]]):
      ProcessorFactory[I, RuleFolderResult[T]] =
    processFactory[I, RuleFolderResult[T]](foldAndReplaceFieldPairsWithStruct(ruleSuite, fields, outputType, debugMode = debugMode,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval, forceTriggerEval = forceTriggerEval), 1, compile,
      forceMutable = forceMutable, extraProjection = extraProjection, enableQualityOptimisations = enableQualityOptimisations,
      forceVarCompilation = forceVarCompilation)(
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
      forceRunnerEval: Boolean = false, forceMutable: Boolean = false,
      extraProjection: DataFrame => DataFrame = identity, enableQualityOptimisations: Boolean = true,
      forceVarCompilation: Boolean = false): ProcessorFactory[I, GeneralExpressionsResult[T]] = {
    import com.sparkutils.quality.implicits._
    implicit val ttyped: TypedEncoder[T] = fromNormalEncoder[T](outputType)
    implicit val enc = TypedExpressionEncoder[GeneralExpressionsResult[T]]
    expressionRunnerFactory[I, T](ruleSuite, outputType = outputType, compile = compile,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval, forceMutable = forceMutable,
      extraProjection = extraProjection, enableQualityOptimisations = enableQualityOptimisations,
      forceVarCompilation = forceVarCompilation)
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
      forceRunnerEval: Boolean = false, forceMutable: Boolean = false,
      extraProjection: DataFrame => DataFrame = identity, enableQualityOptimisations: Boolean = true,
      forceVarCompilation: Boolean = false)
      (implicit resEnc: Encoder[GeneralExpressionsResult[T]]):
      ProcessorFactory[I, GeneralExpressionsResult[T]] =
    processFactory[I, GeneralExpressionsResult[T]](addExpressionRunnerF(ruleSuite, ddlType = outputType.sql,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval), 1, compile, forceMutable = forceMutable,
      extraProjection = extraProjection, enableQualityOptimisations = enableQualityOptimisations,
      forceVarCompilation = forceVarCompilation)(
      implicitly[Encoder[I]], resEnc)

  /**
   * processor for expressionRunner producing Yaml, resulting in GeneralExpressionsResultNoDDL
   * @param input
   * @tparam I
   * @return
   */
  def expressionYamlRunnerFactory[I: Encoder](ruleSuite: RuleSuite, renderOptions: Map[String, String] = Map.empty,
      compile: Boolean = true, compileEvals: Boolean = false,
      forceRunnerEval: Boolean = false, forceMutable: Boolean = false,
      extraProjection: DataFrame => DataFrame = identity, enableQualityOptimisations: Boolean = true,
      forceVarCompilation: Boolean = false):
      ProcessorFactory[I, GeneralExpressionsResult[GeneralExpressionResult]] = {
    import com.sparkutils.quality.implicits._
    processFactory[I, GeneralExpressionsResult[GeneralExpressionResult]](addExpressionRunnerF(ruleSuite, renderOptions = renderOptions,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval), 1, compile, forceMutable = forceMutable,
      extraProjection = extraProjection, enableQualityOptimisations = enableQualityOptimisations,
      forceVarCompilation = forceVarCompilation)(
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
      forceRunnerEval: Boolean = false, forceMutable: Boolean = false,
      extraProjection: DataFrame => DataFrame = identity, enableQualityOptimisations: Boolean = true,
      forceVarCompilation: Boolean = false):
      ProcessorFactory[I, GeneralExpressionsResultNoDDL] = {
    import com.sparkutils.quality.implicits._
    processFactory[I, GeneralExpressionsResultNoDDL](addExpressionRunnerF(ruleSuite, renderOptions = renderOptions,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval, stripDDL = true), 1, compile, forceMutable = forceMutable,
      extraProjection = extraProjection, enableQualityOptimisations = enableQualityOptimisations,
      forceVarCompilation = forceVarCompilation)(
      implicitly[Encoder[I]], generalExpressionsResultNoDDLExpEnc)
  }

}
