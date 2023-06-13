package com.sparkutils.quality.impl

import com.sparkutils.quality.utils.{Docs, RuleSuiteDocs}
import com.sparkutils.quality.utils.RuleSuiteDocs.IdTrEither
import com.sparkutils.quality.{ExpressionLookup, RuleSuite}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame}

trait ValidationImports {

  val emptyDocs = Validation.emptyDocs

  /**
   * For a given dataFrame provide a full set of any validation errors for a given ruleSuite.
   *
   * @param schema which fields should the dataframe have
   * @param ruleSuite
   * @return A set of errors and the output from the dataframe when a runnerFunction is specified
   */
  def validate_Lookup(schema: StructType, ruleSuite: RuleSuite, viewLookup: String => Boolean): (Set[RuleError], Set[RuleWarning]) = {
    val (err, warns, out, docs, exp) = validate(Left(schema), ruleSuite, viewLookup = viewLookup)
    (err, warns)
  }

  /**
   * For a given dataFrame provide a full set of any validation errors for a given ruleSuite.
   *
   * @param schema which fields should the dataframe have
   * @param ruleSuite
   * @return A set of errors and the output from the dataframe when a runnerFunction is specified
   */
  def validate(schema: StructType, ruleSuite: RuleSuite): (Set[RuleError], Set[RuleWarning]) = {
    val (err, warns, out, docs, exp) = validate(Left(schema), ruleSuite)
    (err, warns)
  }

  /**
   * For a given dataFrame provide a full set of any validation errors for a given ruleSuite.
   *
   * @param frame when it's a Left( StructType ) the struct will be used to test against and an emptyDataframe of this type created to resolve on the spark level.  Using Right(DataFrame) will cause that dataframe to be used which is great for test cases with a runnerFunction
   * @param ruleSuite
   * @return A set of errors and the output from the dataframe when a runnerFunction is specified
   */
  def validate_Lookup(frame: DataFrame, ruleSuite: RuleSuite, viewLookup: String => Boolean = Validation.defaultViewLookup): (Set[RuleError], Set[RuleWarning]) = {
    val (err, warns, out, docs, exp) = validate(Right(frame), ruleSuite, viewLookup = viewLookup)
    (err, warns)
  }

  /**
   * For a given dataFrame provide a full set of any validation errors for a given ruleSuite.
   *
   * @param frame when it's a Left( StructType ) the struct will be used to test against and an emptyDataframe of this type created to resolve on the spark level.  Using Right(DataFrame) will cause that dataframe to be used which is great for test cases with a runnerFunction
   * @param ruleSuite
   * @return A set of errors and the output from the dataframe when a runnerFunction is specified
   */
  def validate(frame: DataFrame, ruleSuite: RuleSuite): (Set[RuleError], Set[RuleWarning]) = {
    val (err, warns, out, docs, exp) = validate(Right(frame), ruleSuite)
    (err, warns)
  }

  /**
   * For a given dataFrame provide a full set of any validation errors for a given ruleSuite.
   *
   * @param frame when it's a Left( StructType ) the struct will be used to test against and an emptyDataframe of this type created to resolve on the spark level.  Using Right(DataFrame) will cause that dataframe to be used which is great for test cases with a runnerFunction
   * @param ruleSuite
   * @param runnerFunction - allows you to create a ruleRunner or ruleEngineRunner with different configurations
   * @return A set of errors and the output from the dataframe when a runnerFunction is specified
   *
   */
  def validate_Lookup(frame: DataFrame, ruleSuite: RuleSuite, runnerFunction: DataFrame => Column, viewLookup: String => Boolean): (Set[RuleError], Set[RuleWarning], String, RuleSuiteDocs, Map[IdTrEither, ExpressionLookup]) =
    validate(Right(frame), ruleSuite, runnerFunction = Some(runnerFunction), viewLookup = viewLookup)


  /**
   * For a given dataFrame provide a full set of any validation errors for a given ruleSuite.
   *
   * @param frame when it's a Left( StructType ) the struct will be used to test against and an emptyDataframe of this type created to resolve on the spark level.  Using Right(DataFrame) will cause that dataframe to be used which is great for test cases with a runnerFunction
   * @param ruleSuite
   * @param runnerFunction - allows you to create a ruleRunner or ruleEngineRunner with different configurations
   * @return A set of errors and the output from the dataframe when a runnerFunction is specified
   */
  def validate(frame: DataFrame, ruleSuite: RuleSuite, runnerFunction: DataFrame => Column): (Set[RuleError], Set[RuleWarning], String, RuleSuiteDocs, Map[IdTrEither, ExpressionLookup]) =
    validate(Right(frame), ruleSuite, runnerFunction = Some(runnerFunction))

  /**
   * For a given dataFrame provide a full set of any validation errors for a given ruleSuite.
   *
   * @param frame when it's a Left( StructType ) the struct will be used to test against and an emptyDataframe of this type created to resolve on the spark level.  Using Right(DataFrame) will cause that dataframe to be used which is great for test cases with a runnerFunction
   * @param ruleSuite
   * @param runnerFunction - allows you to create a ruleRunner or ruleEngineRunner with different configurations
   * @param transformBeforeShow - an optional transformation function to help shape what results are pushed to show
   * @return A set of errors and the output from the dataframe when a runnerFunction is specified
   */
  def validate(frame: DataFrame, ruleSuite: RuleSuite, runnerFunction: DataFrame => Column, transformBeforeShow: DataFrame => DataFrame, viewLookup: String => Boolean): (Set[RuleError], Set[RuleWarning], String, RuleSuiteDocs, Map[IdTrEither, ExpressionLookup]) =
    validate(Right(frame), ruleSuite, runnerFunction = Some(runnerFunction), transformBeforeShow = transformBeforeShow, viewLookup = viewLookup)

  /**
   * For a given dataFrame provide a full set of any validation errors for a given ruleSuite.
   *
   * @param frame when it's a Left( StructType ) the struct will be used to test against and an emptyDataframe of this type created to resolve on the spark level.  Using Right(DataFrame) will cause that dataframe to be used which is great for test cases with a runnerFunction
   * @param ruleSuite
   * @param runnerFunction - allows you to create a ruleRunner or ruleEngineRunner with different configurations
   * @param transformBeforeShow - an optional transformation function to help shape what results are pushed to show
   * @return A set of errors and the output from the dataframe when a runnerFunction is specified
   */
  def validate(frame: DataFrame, ruleSuite: RuleSuite, runnerFunction: DataFrame => Column, transformBeforeShow: DataFrame => DataFrame): (Set[RuleError], Set[RuleWarning], String, RuleSuiteDocs, Map[IdTrEither, ExpressionLookup]) =
    validate(Right(frame), ruleSuite, runnerFunction = Some(runnerFunction), transformBeforeShow = transformBeforeShow)

  /**
   * For a given dataFrame provide a full set of any validation errors for a given ruleSuite.
   *
   * @param schemaOrFrame when it's a Left( StructType ) the struct will be used to test against and an emptyDataframe of this type created to resolve on the spark level.  Using Right(DataFrame) will cause that dataframe to be used which is great for test cases with a runnerFunction
   * @param ruleSuite
   * @param runnerFunction - allows you to create a ruleRunner or ruleEngineRunner with different configurations
   * @param showParams - configure how the output text is formatted using the same options and formatting as dataFrame.show
   * @param qualityName - the column name to store the runnerFunction results in
   * @param recursiveLambdasSOEIsOk - this signals that finding a recursive lambda SOE should not stop the evaluations - if true it will still try to run any runnerFunction but may not give the correct results
   * @param transformBeforeShow - an optional transformation function to help shape what results are pushed to show
   * @param viewLookup - for any subquery used looks up the view name for being present (quoted and with schema), defaults to the current spark catalogue
   * @return A set of errors and the output from the dataframe when a runnerFunction is specified
   */
  def validate(schemaOrFrame: Either[StructType, DataFrame], ruleSuite: RuleSuite, showParams: ShowParams = ShowParams(),
               runnerFunction: Option[DataFrame => Column] = None, qualityName: String = "Quality",
               recursiveLambdasSOEIsOk: Boolean = false, transformBeforeShow: DataFrame => DataFrame = identity, viewLookup: String => Boolean = Validation.defaultViewLookup):
  (Set[RuleError], Set[RuleWarning], String, RuleSuiteDocs, Map[IdTrEither, ExpressionLookup]) = Validation.validate(
    schemaOrFrame, ruleSuite, showParams, runnerFunction, qualityName,
    recursiveLambdasSOEIsOk, transformBeforeShow, viewLookup
  )
}
