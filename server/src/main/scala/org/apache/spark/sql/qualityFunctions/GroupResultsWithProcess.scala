package org.apache.spark.sql.qualityFunctions

import com.sparkutils.quality.QualityException
import com.sparkutils.quality.impl.GroupResults.rd
import com.sparkutils.quality.impl.{GroupResults, GroupResultsBase, RuleLogicUtils}
import com.sparkutils.shim.expressions.HigherOrderFunctionLike
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Expression, HigherOrderFunction, LambdaFunction, NamedLambdaVariable, UnresolvedNamedLambdaVariable}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{AbstractDataType, ArrayType, DataType}

case class GroupResultsWithProcess(arguments: Seq[Expression], function: Expression) extends GroupResultsBase with HigherOrderFunctionLike {
  val hasProcessor: Boolean = true

  def children =  arguments :+ function

  lazy val ref = arguments.last.asInstanceOf[RefExpression]

  override def argumentTypes: Seq[AbstractDataType] = arguments.map(_.dataType)

  override def functions: Seq[Expression] = Seq(function)

  override def functionTypes: Seq[AbstractDataType] = Seq(function.dataType)

  // forces function.dataType to be called before it's resolved
//  override def checkInputDataTypes(): TypeCheckResult = TypeCheckResult.TypeCheckSuccess

  override def processResultType: DataType = function.dataType
  override def processResultNullable: Boolean = function.nullable

  override protected def bindInternal(f: (Expression, Seq[(DataType, Boolean)]) => LambdaFunction): HigherOrderFunction = {
    // subqueries aren't being replaced correctly
    val res = copy(function = f(function,
      Seq((arguments.last.dataType, arguments.last.nullable))))

    if (RuleLogicUtils.hasSubQuery(res.function)) {
      // only possible on > 3.4 (and DBR 12.2),
      // no longer possible after 14.3/4.0, this code won't be reached due to https://issues.apache.org/jira/browse/SPARK-47509
      // unless it's re-enabled
      // given XX below reject this occurrence directly.
      if (arguments.last.collect { case u: UnresolvedNamedLambdaVariable => u }.nonEmpty) {
        QualityException.qualityException(s"Cannot use LambdaFunctions with SubqueryExpressions and parameters containing lambdavariables " + this)
      }

      val converted = SubQueryLambda.convertLambdaFunction(res.function)(function, Seq(arguments.last))

      res.copy(function = converted)
    } else
      res

  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren.dropRight(1), newChildren.last)

  @transient lazy val LambdaFunction(lambdaFunction, elementNamedVariables, _) = function
  @transient lazy val elementVars = elementNamedVariables

  override def processResult(row: InternalRow, a: ArrayData): Any = {
    // set up the variable to be evaluated
    elementVars.head match {
        case nlv: NamedLambdaVariable => nlv.value.set(a)
        case nlvg: NamedLambdaVariableCodeGen => nlvg.value = a
      }

    function.eval(row)
  }

}
