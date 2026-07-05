package org.apache.spark.sql.qualityFunctions

import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * Pre 3.2 it is not possible to use Folder with grouping as grouping forces subExpr usage of FunN_X code.  3.2
 * adds filtering from subExpr usage for [[TreePattern]] [[LAMBDA_VARIABLE]] patterns to stop EquivalentExpressions.
 * Disabling this can be observed with fails on RuleFolderClassicWithTopLevelGrouperTest.testSimpleProductionRules.
 *
 * Pre32 has empty for compilation
 */
trait LambdaVariablePattern extends Expression {

}
