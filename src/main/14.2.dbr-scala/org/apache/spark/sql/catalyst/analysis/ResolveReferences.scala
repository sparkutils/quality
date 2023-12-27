package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.connector.catalog.{View => _, _}

// 14 dbr uses https://issues.apache.org/jira/browse/SPARK-42849
class ResolveReferences(val catalogManager: CatalogManager)
  extends Rule[LogicalPlan] with ColumnResolutionHelper {
  def apply(plan: LogicalPlan): LogicalPlan = ???
}