package com.sparkutils.quality.sparkless

import com.sparkutils.quality.impl.Encoders.rowTypedEnc
import com.sparkutils.quality.impl.util.EmbeddedTypeCorrection.{noCorrection, ofRuleEngine, ofRuleFolder}
import com.sparkutils.quality.impl.{Encoders, LazyRuleSuiteResultDetailsImpl, LazyRuleSuiteResultDetailsProxyImpl, LazyRuleSuiteResultImpl}
import com.sparkutils.quality.impl.util.Encoding.fromNormalEncoder
import com.sparkutils.quality.sparkless.StarUtil.star
import com.sparkutils.quality.{LazyRuleEngineResult, LazyRuleFolderResult, LazyRuleSuiteResultDetails, Passed, RuleResult, RuleSuite, RuleSuiteResultDetails, SalientRule, foldAndReplaceFieldPairsWithStruct, foldAndReplaceFieldsWithStruct, ruleEngineWithStructF}
import com.sparkutils.quality.sparkless.impl.Processors.processFactory
import frameless.{TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.sql.{Column, DataFrame, Encoder, Row}
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * The lazy functions require knowledge and direct use of internal row and are unsuitable for 4.1 / AgnosticEncoders
 */
trait LazyProcessFunctions { self: NonLazyProcessFunctions =>

  /**
   * processor for DQ rules returning a pair of overall RuleResult and a LazyRuleSuiteResultDetails
   * @tparam I
   * @return
   */
  def lazyDQDetailsFactory[I: Encoder](ruleSuite: RuleSuite, compile: Boolean = true, compileEvals: Boolean = false,
                                       forceRunnerEval: Boolean = false, forceMutable: Boolean = false,
                                       extraProjection: DataFrame => DataFrame = identity, enableQualityOptimisations: Boolean = true,
                                       forceVarCompilation: Boolean = false, defaultIfPassed: Option[RuleSuiteResultDetails] = None):
  ProcessorFactory[I, (RuleResult, LazyRuleSuiteResultDetails)] = {
    import com.sparkutils.quality.implicits._
    implicit val rowEnc = rowTypedEnc(Encoders.ruleSuiteResultDetailsTypedEnc.catalystRepr)

    implicit val tup = TypedEncoder[(RuleResult, Row)].agnosticEncoder
    val defaultIfPassedProxy = defaultIfPassed.map(LazyRuleSuiteResultDetailsProxyImpl(_))

    val iEnc = implicitly[Encoder[I]]

    val r = processFactory[I, (RuleResult, Row)](addOverallResultsAndDetailsWrapperF(ruleSuite,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval), noCorrection, compile, forceMutable = forceMutable,
      extraProjection = extraProjection, enableQualityOptimisations = enableQualityOptimisations,
      forceVarCompilation = forceVarCompilation)

    ProcessorFactoryProxy(r, (p: (RuleResult, Row)) => {
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
   * processor for ruleEngine with encoding over the nested T in RuleEngineResult[T] with lazy serialisation of the RuleSuiteResult.
   * *Note* you must use AgnosticEncoders in 3.4+ in order to be able to support Java collections/generics,
   * reflection via .bean is not sufficient for java generics.
   * @tparam I the input type
   * @tparam T the result type of the rule engine
   * @return
   */
  def lazyRuleEngineFactory[I: Encoder, T: Encoder](ruleSuite: RuleSuite, compile: Boolean = true,
                                                    compileEvals: Boolean = false,
                                                    forceRunnerEval: Boolean = false, forceTriggerEval: Boolean = false, forceMutable: Boolean = false,
                                                    extraProjection: DataFrame => DataFrame = identity, enableQualityOptimisations: Boolean = true,
                                                    forceVarCompilation: Boolean = false)(implicit oenc: Encoder[Option[T]]): ProcessorFactory[I, LazyRuleEngineResult[T]] = {
    import com.sparkutils.quality.implicits._

    implicit val rowEnc: TypedEncoder[Row] = rowTypedEnc(ruleSuiteResultTypedEnc.catalystRepr)

    import com.sparkutils.quality.impl.util.Encoding._
    implicit val enc = TypedEncoder[(Row, Option[SalientRule], Option[T])].agnosticEncoder

    val r = processFactory[I, (Row, Option[SalientRule], Option[T])](star("ruleEngine")(ruleEngineWithStructF(ruleSuite,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval, forceTriggerEval = forceTriggerEval)), ofRuleEngine[Option[T]], compile,
      forceMutable = forceMutable, extraProjection = extraProjection, enableQualityOptimisations = enableQualityOptimisations,
      forceVarCompilation = forceVarCompilation)

    ProcessorFactoryProxy(r, (p: ((Row, Option[SalientRule], Option[T]))) => {
      LazyRuleEngineResult(LazyRuleSuiteResultImpl(p._1), p._2, p._3)
    })
  }


  /**
   * processor for ruleFolder with encoding over the nested T in RuleFolderResult[T], lazily serialising the RuleSuiteResults
   * *Note* you must use AgnosticEncoders in 3.4+ in order to be able to support Java collections/generics,
   * reflection via .bean is not sufficient for java generics.
   * @tparam I the input type
   * @tparam T the result type of the rule engine
   * @return
   */
  def lazyRuleFolderFactoryWithStructStarter[I: Encoder, T: Encoder](ruleSuite: RuleSuite, fields: Seq[(String, Column)],
                                                                     outputType: StructType, compile: Boolean = true, compileEvals: Boolean = false,
                                                                     forceRunnerEval: Boolean = false, forceTriggerEval: Boolean = false, forceMutable: Boolean = false,
                                                                     extraProjection: DataFrame => DataFrame = identity, enableQualityOptimisations: Boolean = true,
                                                                     forceVarCompilation: Boolean = false): ProcessorFactory[I, LazyRuleFolderResult[T]] = {
    import com.sparkutils.quality.implicits._
    implicit val rowEnc = rowTypedEnc(Encoders.ruleSuiteResultTypedEnc.catalystRepr)

    import com.sparkutils.quality.impl.util.Encoding._
    implicit val enc = TypedEncoder[(Row, Option[T])].agnosticEncoder

    val r = processFactory[I, (Row, Option[T])](star("foldedFields")(foldAndReplaceFieldPairsWithStruct(ruleSuite, fields, outputType,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval, forceTriggerEval = forceTriggerEval)), ofRuleFolder[T], compile,
      forceMutable = forceMutable, extraProjection = extraProjection, enableQualityOptimisations = enableQualityOptimisations,
      forceVarCompilation = forceVarCompilation)

    ProcessorFactoryProxy(r, (p: ((Row, Option[T]))) => {
      LazyRuleFolderResult(LazyRuleSuiteResultImpl(p._1), p._2)
    })
  }


  /**
   * processor for ruleFolder with encoding over the nested T in RuleFolderResult[T], lazy serialising RuleSuiteResults.
   * *Note* you must use AgnosticEncoders in 3.4+ in order to be able to support Java collections/generics,
   * reflection via .bean is not sufficient for java generics.
   * @tparam I the input type
   * @tparam T the result type of the rule engine
   * @return
   */
  def lazyRuleFolderFactory[I: Encoder, T: Encoder](ruleSuite: RuleSuite, outputType: StructType, compile: Boolean = true,
                                                    compileEvals: Boolean = false,
                                                    forceRunnerEval: Boolean = false, forceTriggerEval: Boolean = false, forceMutable: Boolean = false,
                                                    extraProjection: DataFrame => DataFrame = identity, enableQualityOptimisations: Boolean = true,
                                                    forceVarCompilation: Boolean = false): ProcessorFactory[I, LazyRuleFolderResult[T]] = {
    import com.sparkutils.quality.implicits._
    implicit val rowEnc = rowTypedEnc(Encoders.ruleSuiteResultTypedEnc.catalystRepr)

    import com.sparkutils.quality.impl.util.Encoding._
    implicit val enc = TypedEncoder[(Row, Option[T])].agnosticEncoder

// TODO this doesn't seem right, same function def?
    val r = processFactory[I, (Row, Option[T])](star("foldedFields")(foldAndReplaceFieldsWithStruct(ruleSuite, outputType,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval, forceTriggerEval = forceTriggerEval)), ofRuleFolder[T], compile,
      forceMutable = forceMutable, extraProjection = extraProjection, enableQualityOptimisations = enableQualityOptimisations,
      forceVarCompilation = forceVarCompilation)

    ProcessorFactoryProxy(r, (p: ((Row, Option[T]))) => {
      LazyRuleFolderResult(LazyRuleSuiteResultImpl(p._1), p._2)
    })
  }
}