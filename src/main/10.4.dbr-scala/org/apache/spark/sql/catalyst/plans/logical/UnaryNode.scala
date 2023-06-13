package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, EqualNullSafe, Expression, ExpressionSet, Literal, NamedExpression, SortOrder, WindowExpression}
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, TreePattern}
import org.apache.spark.sql.catalyst.trees.UnaryLike

abstract class UnaryNode extends LogicalPlan with UnaryLike[LogicalPlan] {
  /**
   * Generates all valid constraints including an set of aliased constraints by replacing the
   * original constraint expressions with the corresponding alias
   */
  protected def getAllValidConstraints(projectList: Seq[NamedExpression]): ExpressionSet = {
    var allConstraints = child.constraints
    projectList.foreach {
      case a @ Alias(l: Literal, _) =>
        allConstraints += EqualNullSafe(a.toAttribute, l)
      case a @ Alias(e, _) =>
        // For every alias in `projectList`, replace the reference in constraints by its attribute.
        allConstraints ++= allConstraints.map(_ transform {
          case expr: Expression if expr.semanticEquals(e) =>
            a.toAttribute
        })
        allConstraints += EqualNullSafe(e, a.toAttribute)
      case _ => // Don't change.
    }

    allConstraints
  }

  override protected lazy val validConstraints: ExpressionSet = child.constraints
}

abstract class OrderPreservingUnaryNode extends UnaryNode {
  override final def outputOrdering: Seq[SortOrder] = child.outputOrdering
}

case class Aggregate(
                      groupingExpressions: Seq[Expression],
                      aggregateExpressions: Seq[NamedExpression],
                      child: LogicalPlan)
  extends UnaryNode {

  override lazy val resolved: Boolean = {
    val hasWindowExpressions = aggregateExpressions.exists ( _.collect {
      case window: WindowExpression => window
    }.nonEmpty
    )

    !expressions.exists(!_.resolved) && childrenResolved && !hasWindowExpressions
  }

  override def output: Seq[Attribute] = aggregateExpressions.map(_.toAttribute)
  override def metadataOutput: Seq[Attribute] = Nil
  override def maxRows: Option[Long] = {
    if (groupingExpressions.isEmpty) {
      Some(1L)
    } else {
      child.maxRows
    }
  }

  final override val nodePatterns : Seq[TreePattern] = Seq(AGGREGATE)

  override lazy val validConstraints: ExpressionSet = {
    val nonAgg = aggregateExpressions.filter(_.find(_.isInstanceOf[AggregateExpression]).isEmpty)
    getAllValidConstraints(nonAgg)
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): Aggregate =
    copy(child = newChild)

  // Whether this Aggregate operator is group only. For example: SELECT a, a FROM t GROUP BY a
  private[sql] def groupOnly: Boolean = {
    aggregateExpressions.forall(a => groupingExpressions.exists(g => a.semanticEquals(g)))
  }
}
