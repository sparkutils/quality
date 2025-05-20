package com.sparkutils.quality.impl.util

import com.sparkutils.quality.RuleSuite
import com.sparkutils.quality.functions.strip_result_ddl
import com.sparkutils.quality.impl.util.AddDataFunctions.ifoldAndReplaceFields
import com.sparkutils.quality.impl.{ExpressionRunner, RuleEngineRunnerImpl, RuleRunnerImpl}
import org.apache.spark.sql.QualitySparkUtils.DatasetBase
import org.apache.spark.sql.{Column, DataFrame, Row => SRow}
import org.apache.spark.sql.types.{DataType, StructType}

trait AddDataFunctionsImports {


  /**
   * Adds a DataQuality field using the RuleSuite and RuleSuiteResult structure
   * @param dataFrame
   * @param rules
   * @param name
   * @return
   */
  def addDataQuality(dataFrame: DataFrame, rules: RuleSuite, name: String = "DataQuality", compileEvals: Boolean = false,
                     forceRunnerEval: Boolean = false): DataFrame = {
    import org.apache.spark.sql.functions.expr
    dataFrame.select(expr("*"), RuleRunnerImpl.ruleRunnerImpl(rules, compileEvals = compileEvals,
      forceRunnerEval = forceRunnerEval).as(name))
  }

  /**
   * Adds a DataQuality field using the RuleSuite and RuleSuiteResult structure for use with dataset.transform functions
   * @param rules
   * @param name
   * @return
   */
  def addDataQualityF(rules: RuleSuite, name: String = "DataQuality", compileEvals: Boolean = false,
                      forceRunnerEval: Boolean = false): DataFrame => DataFrame =
    addDataQuality(_, rules, name, compileEvals = compileEvals,
      forceRunnerEval = forceRunnerEval)

  /**
   * Adds two columns, one for overallResult and the other the details, allowing 30-50% performance gains for simple filters
   * @param dataFrame
   * @param rules
   * @param overallResult
   * @param resultDetails
   * @return
   */
  def addOverallResultsAndDetails(dataFrame: DataFrame, rules: RuleSuite, overallResult: String = "DQ_overallResult",
                                  resultDetails: String = "DQ_Details", compileEvals: Boolean = false,
                                  forceRunnerEval: Boolean = false): DataFrame = {
    val temporaryDQname: String = "DQ_TEMP_Quality"
    addDataQuality(dataFrame, rules, temporaryDQname, compileEvals = compileEvals,
      forceRunnerEval = forceRunnerEval).
      selectExpr("*",s"$temporaryDQname.overallResult as $overallResult",
        s"ruleSuiteResultDetails($temporaryDQname) as $resultDetails").drop(temporaryDQname)
  }

  /**
   * Adds two columns, one for overallResult and the other the details, allowing 30-50% performance gains for simple filters, for use in dataset.transform functions
   * @param rules
   * @param overallResult
   * @param resultDetails
   * @return
   */
  def addOverallResultsAndDetailsF(rules: RuleSuite, overallResult: String = "DQ_overallResult",
                                   resultDetails: String = "DQ_Details", compileEvals: Boolean = false,
                                   forceRunnerEval: Boolean = false): DataFrame=> DataFrame =
    addOverallResultsAndDetails(_, rules, overallResult, resultDetails, compileEvals = compileEvals,
      forceRunnerEval = forceRunnerEval)

  /**
   * Leverages the foldRunner to replace fields, the input fields are used to create a structure that the rules fold over.
   * The fields are then dropped from the original Dataframe and added back from the resulting structure.
   *
   * NOTE: The field order and types of the original DF will be maintained only when maintainOrder is true.  As it requires access to the schema it may incur extra work.
   *
   * @param rules
   * @param debugMode when true the last results are taken for the replaced fields
   * @param maintainOrder when true the schema is used to replace fields in the correct location, when false they are simply appended
   * @return
   */
  def foldAndReplaceFields[P[R] >: DatasetBase[R]](rules: RuleSuite, fields: Seq[String], foldFieldName: String = "foldedFields",
      debugMode: Boolean = false, tempFoldDebugName: String = "tempFOLDDEBUG",
      maintainOrder: Boolean = true, compileEvals: Boolean = false, forceRunnerEval: Boolean = false,
      forceTriggerEval: Boolean = false): P[SRow] => P[SRow] =
    ifoldAndReplaceFields(rules, Left(fields), foldFieldName,
      debugMode, tempFoldDebugName, maintainOrder, compileEvals = compileEvals,
      forceRunnerEval = forceRunnerEval, forceTriggerEval = forceTriggerEval)

  /**
   * Leverages the foldRunner to replace fields, the input fields pairs are used to create a structure that the rules fold over.
   * Any field references in the providing pairs are then dropped from the original Dataframe and added back from the resulting structure.
   *
   * NOTE: The field order and types of the original DF will be maintained only when maintainOrder is true.  As it requires access to the schema it may incur extra work.
   *
   * @param rules
   * @param debugMode when true the last results are taken for the replaced fields
   * @param maintainOrder when true the schema is used to replace fields in the correct location, when false they are simply appended
   * @return
   */
  def foldAndReplaceFieldPairs[P[R] >: DatasetBase[R]](rules: RuleSuite, fields: Seq[(String, Column)], foldFieldName: String = "foldedFields",
                                                   debugMode: Boolean = false, tempFoldDebugName: String = "tempFOLDDEBUG",
                                                   maintainOrder: Boolean = true, compileEvals: Boolean = false, forceRunnerEval: Boolean = false,
                                                   forceTriggerEval: Boolean = false): P[SRow] => P[SRow] =
    ifoldAndReplaceFields(rules, Right(fields), foldFieldName,
      debugMode, tempFoldDebugName, maintainOrder, compileEvals = compileEvals,
      forceRunnerEval = forceRunnerEval, forceTriggerEval = forceTriggerEval)

  /**
   * Leverages the foldRunner to replace fields, the input fields are used to create a structure that the rules fold over.
   * The fields are then dropped from the original Dataframe and added back from the resulting structure.
   *
   * This version should only be used when you require select(*, foldRunner) to be used, it requires you fully specify types.
   *
   * NOTE: The field order and types of the original DF will be maintained only when maintainOrder is true.  As it requires access to the schema it may incur extra work.
   *
   * @param rules
   * @param struct The fields, and types, are used to call the foldRunner.  These types must match in the input fields
   * @param debugMode when true the last results are taken for the replaced fields
   * @param maintainOrder when true the schema is used to replace fields in the correct location, when false they are simply appended
   * @return
   */
  def foldAndReplaceFieldsWithStruct[P[R] >: DatasetBase[R]](rules: RuleSuite, struct: StructType, foldFieldName: String = "foldedFields",
      debugMode: Boolean = false, tempFoldDebugName: String = "tempFOLDDEBUG",
      maintainOrder: Boolean = true, compileEvals: Boolean = false,
      forceRunnerEval: Boolean = false, forceTriggerEval: Boolean = false): P[SRow] => P[SRow] =
    AddDataFunctions.ifoldAndReplaceFields(rules, Left(struct.fields.map(_.name)), foldFieldName,
      debugMode, tempFoldDebugName, maintainOrder, useType = Some(struct), compileEvals = compileEvals,
      forceRunnerEval = forceRunnerEval, forceTriggerEval = forceTriggerEval)

  /**
   * Leverages the foldRunner to replace fields, the input field pairs are used to create a structure that the rules fold over,
   * the type must match the provided struct.
   * Any field references in the providing pairs are then dropped from the original Dataframe and added back from the resulting structure.
   *
   * This version should only be used when you require select(*, foldRunner) to be used, it requires you fully specify types.
   *
   * NOTE: The field order and types of the original DF will be maintained only when maintainOrder is true.  As it requires access to the schema it may incur extra work.
   *
   * @param rules
   * @param struct The fields, and types, are used to call the foldRunner.  These types must match in the input fields
   * @param debugMode when true the last results are taken for the replaced fields
   * @param maintainOrder when true the schema is used to replace fields in the correct location, when false they are simply appended
   * @return
   */
  def foldAndReplaceFieldPairsWithStruct[P[R] >: DatasetBase[R]](rules: RuleSuite, fields: Seq[(String, Column)], struct: StructType, foldFieldName: String = "foldedFields",
                                                             debugMode: Boolean = false, tempFoldDebugName: String = "tempFOLDDEBUG",
                                                             maintainOrder: Boolean = true, compileEvals: Boolean = false,
                                                             forceRunnerEval: Boolean = false, forceTriggerEval: Boolean = false): P[SRow] => P[SRow] =
    AddDataFunctions.ifoldAndReplaceFields(rules, Right(fields), foldFieldName,
      debugMode, tempFoldDebugName, maintainOrder, useType = Some(struct), compileEvals = compileEvals,
      forceRunnerEval = forceRunnerEval, forceTriggerEval = forceTriggerEval)

  /**
   * Leverages the ruleRunner to produce an new output structure, the outputType defines the output structure generated.
   *
   * This version should only be used when you require select(*, ruleRunner) to be used, it requires you fully specify types.
   *
   * @param rules
   * @param dataFrame the input dataframe
   * @param outputType The fields, and types, are used to call the foldRunner.  These types must match in the input fields
   * @param ruleEngineFieldName The field name the results will be stored in, by default ruleEngine
   * @param alias sets the alias to use for dataFrame when using subqueries to resolve ambiguities, setting to an empty string (or null) will not assign an alias
   * @return
   */
  def ruleEngineWithStruct(dataFrame: DataFrame, rules: RuleSuite, outputType: DataType,
      ruleEngineFieldName: String = "ruleEngine", alias: String = "main", debugMode: Boolean = false,
      compileEvals: Boolean = false, forceRunnerEval: Boolean = false, forceTriggerEval: Boolean = false): DataFrame = {
    import org.apache.spark.sql.functions.expr
    (if ( (alias eq null) || alias.isEmpty )
      dataFrame
    else
      dataFrame.as(alias)).select(expr("*"), RuleEngineRunnerImpl.ruleEngineRunnerImpl(rules, outputType, debugMode = debugMode,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval, forceTriggerEval = forceTriggerEval).as(ruleEngineFieldName))
  }

  /**
   * Leverages the ruleRunner to produce an new output structure, the outputType defines the output structure generated.
   *
   * This version should only be used when you require select(*, ruleRunner) to be used, it requires you fully specify types.
   *
   * @param rules
   * @param outputType The fields, and types, are used to call the foldRunner.  These types must match in the input fields
   * @return
   */
  def ruleEngineWithStructF[P[R] >: DatasetBase[R]](rules: RuleSuite, outputType: DataType,
      ruleEngineFieldName: String = "ruleEngine", alias: String = "main", debugMode: Boolean = false,
      compileEvals: Boolean = false, forceRunnerEval: Boolean = false, forceTriggerEval: Boolean = false): P[SRow] => P[SRow] =
    (p: P[SRow]) => ruleEngineWithStruct(p.asInstanceOf[DataFrame], rules, outputType, ruleEngineFieldName, alias, debugMode,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval, forceTriggerEval = forceTriggerEval).asInstanceOf[P[SRow]]

  /**
   * Runs the ruleSuite expressions saving results as a tuple of (ruleResult: yaml, resultType: String)
   * Supplying a ddlType triggers the output type for the expression to be that ddl type, rather than using yaml conversion.
   * @param renderOptions provides rendering options to the underlying snake yaml implementation
   * @param ddlType optional DDL string, when present yaml output is disabled and the output expressions must all have the same type
   * @param name the default column name "expressionResults"
   */
  def addExpressionRunner(dataFrame: DataFrame, ruleSuite: RuleSuite, name: String = "expressionResults",
                       renderOptions: Map[String, String] = Map.empty, ddlType: String = "",
                       forceRunnerEval: Boolean = false, compileEvals: Boolean = false, stripDDL: Boolean = false): DataFrame = {
    import org.apache.spark.sql.functions.expr
    val runner =
      ExpressionRunner(ruleSuite, name = name, renderOptions = renderOptions,
      ddlType = ddlType,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval)

    dataFrame.select(expr("*"),
      if (stripDDL)
        strip_result_ddl(runner)
      else
        runner
    )
  }

  /**
   * Runs the ruleSuite expressions saving results as a tuple of (ruleResult: yaml, resultType: String)
   * Supplying a ddlType triggers the output type for the expression to be that ddl type, rather than using yaml conversion.
   * @param renderOptions provides rendering options to the underlying snake yaml implementation
   * @param ddlType optional DDL string, when present yaml output is disabled and the output expressions must all have the same type
   * @param name the default column name "expressionResults"
   */
  def addExpressionRunnerF[P[R] >: DatasetBase[R]](ruleSuite: RuleSuite, name: String = "expressionResults",
                       renderOptions: Map[String, String] = Map.empty, ddlType: String = "",
                       forceRunnerEval: Boolean = false, compileEvals: Boolean = false, stripDDL: Boolean = false): P[SRow] => P[SRow] =
    (p: P[SRow]) => {
      import org.apache.spark.sql.functions.expr
      addExpressionRunner(p.asInstanceOf[DataFrame], ruleSuite, name = name, renderOptions = renderOptions,
        ddlType = ddlType,
        compileEvals = compileEvals, forceRunnerEval = forceRunnerEval, stripDDL = stripDDL).asInstanceOf[P[SRow]]
    }
}
