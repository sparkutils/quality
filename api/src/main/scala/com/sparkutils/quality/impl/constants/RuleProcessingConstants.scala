package com.sparkutils.quality.impl.constants

trait RuleProcessingConstants {

  /**
   * TopLevelBooleanGrouper is a [[groupProcessorKey]] that groups by Booleans in rules that can be safely grouped,
   * for example And, EqualTo and other BooleanType expressions, the Or disjunction is explicitly not supported.  These
   * groups can be further optimised by differentiating expressions, currently only EqualTo with a literal is supported
   * and will generate hash based buckets.
   */
  val topLevelBooleanGrouper = "com.sparkutils.quality.impl.TopLevelBooleanGrouper"

  /**
   * Processing implementation for trigger grouping e.g. [[topLevelBooleanGrouper]].  This is used by all engine runners to access their
   * extraConfig Map, then via getConfig and finally the runner appropriate default.
   */
  val groupProcessorKey = "quality.runnerGroupProcessor"

  /**
   * When present and true will trigger the dumpAudit process for a given group processing implementation.  By
   * default, no dumpAudit is run.
   */
  val groupProcessorDumpAuditKey = "quality.runnerGroupProcessor.dumpAudit"

  /**
   * Defaulting to 130, the default bucket size can be overridden.  NOTE this size is a guide to bucketing only, use
   * dumpAudit via [[groupProcessorDumpAuditKey]] to identify the optimum size for a given rule suite, this can take many
   * minutes to run.
   */
  val groupProcessorBucketSizeKey = "quality.runnerGroupProcessor.bucketSize"

  /**
   * Defaulting to 0.010, this filter size can be overridden.  NOTE this is a guide to bucketing only and implies
   * what percentage a given sub expression should be filtered out from grouping. e.g. If the subexpression is only
   * used in less than 0.1% of the trigger rules then it should not be used as a grouping tool.
   */
  val groupProcessorPercentFilter = "quality.runnerGroupProcessor.percentFilter"

  /**
   * Defaulting to "./", specifies the location of where to save any audit files
   */
  val groupProcessorAuditLocation = "quality.runnerGroupProcessor.auditLocation"

  /**
   * Defaulting to "classSimpleName", specifies the name for the audit file
   */
  val groupProcessorAuditName = "quality.runnerGroupProcessor.auditName"

  /**
   * Dumps compilation time for ruleRunners with SplitCompilation
   */
  val showSplitCompilationTime = "quality.showSplitCompilationTime"

  /**
   * Dumps grouping time
   */
  val showGroupingTime = "quality.showGroupingTime"

  /**
   * When true creates a ruleSuiteResults field without any an empty ruleSetResults.  This is only used by ruleEngine,
   * collector and ruleFolder and will decrease processing and storage time considerably for large RuleSuites
   * (Quality rule processing no longer has to copy each input row for every rule and Spark no longer has to write the
   * larger datastructures).
   * When using RuleEngine salientRule may provide enough audit information for a given use case, but for folder and
   * collector [[useEvaluatedOnlyRuleSetResults]] may be more appropriate.
   */
  val useEmptyRuleSetResults = "quality.useEmptyRuleSetResults"

  /**
   * When true creates a ruleSuiteResults field without any an empty ruleSetResults.  This is only used by ruleEngine,
   * collector and ruleFolder and will decrease processing and storage time considerably for large RuleSuites using
   * TopLevelBoolean
   * (Quality rule processing no longer has to copy each input row for every rule and Spark no longer has to write the
   * larger datastructures)
   */
  val useEvaluatedOnlyRuleSetResults = "quality.useEvaluatedOnlyRuleSetResults"
}
