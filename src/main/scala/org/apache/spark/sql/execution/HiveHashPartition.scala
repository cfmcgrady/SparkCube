// scalastyle:off
package org.apache.spark.sql.execution

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions, Strategy}
import org.apache.spark.sql.catalyst.expressions.{Expression, HiveHash, Literal, Pmod, SortOrder, Unevaluable}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, RepartitionByExpression}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}
import org.apache.spark.sql.execution.BucketInfoExtensions.ExtensionsBuilder
import org.apache.spark.sql.execution.SparkPlan

class RepartitionByHiveHashExpression(partitionExpressionsx: Seq[Expression],
                                      childx: LogicalPlan,
                                      numPartitionsx: Int) extends
  RepartitionByExpression(partitionExpressionsx, childx, numPartitionsx) {

  override val partitioning: Partitioning = {
    val (sortOrder, nonSortOrder) = partitionExpressions.partition(_.isInstanceOf[SortOrder])

    require(sortOrder.isEmpty || nonSortOrder.isEmpty,
      s"${getClass.getSimpleName} expects that either all its `partitionExpressions` are of type " +
        "`SortOrder`, which means `RangePartitioning`, or none of them are `SortOrder`, which " +
        "means `HashPartitioning`. In this case we have:" +
        s"""
           |SortOrder: $sortOrder
           |NonSortOrder: $nonSortOrder
       """.stripMargin)
    new HiveHashPartitioningV2(nonSortOrder, numPartitions)
  }

}

class HiveHashPartitioningV2(expressionsx: Seq[Expression], numPartitionsx: Int)
  extends HashPartitioning(expressionsx, numPartitionsx)
    with Partitioning with Unevaluable {
  /**
   * Returns an expression that will produce a valid partition ID(i.e. non-negative and is less
   * than numPartitions) based on hashing expressions.
   */
  override def partitionIdExpression: Expression = Pmod(HiveHash(expressions), Literal(numPartitions))
}

class RepartitionByHiveHashStrategy extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case r: RepartitionByHiveHashExpression =>
        exchange.ShuffleExchangeExec(
          r.partitioning, planLater(r.child)) :: Nil
      case _ => Nil
    }
  }
}

class PartitionExtensions extends ExtensionsBuilder with Logging {
  override def apply(sessionExtensions: SparkSessionExtensions): Unit = {
    logInfo("register extension PartitionExtensions.")
    sessionExtensions.injectPlannerStrategy(_ => new RepartitionByHiveHashStrategy)
    sessionExtensions.injectOptimizerRule(_ => ReplaceHadoopFsRelation())
  }
}

object BucketInfoExtensions {
  type ParserBuilder = (SparkSession, ParserInterface) => ParserInterface
  type ExtensionsBuilder = SparkSessionExtensions => Unit
}

