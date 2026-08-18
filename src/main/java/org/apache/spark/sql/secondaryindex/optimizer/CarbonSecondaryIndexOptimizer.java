package org.apache.spark.sql.secondaryindex.optimizer;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * No-op replacement for CarbonData's Spark 3.1 secondary-index optimizer.
 * <p>
 * The upstream {@code carbondata-spark_3.1} class fails JVM bytecode verification on Spark 3.2+
 * ({@code VerifyError}: {@code UnaryNode} is not assignable to {@code LogicalPlan}).
 * This bench uses Bloom/Lucene indexes, not legacy secondary-index plan rewrite.
 */
public final class CarbonSecondaryIndexOptimizer {

    public CarbonSecondaryIndexOptimizer(SparkSession sparkSession) {
    }

    public LogicalPlan transformFilterToJoin(LogicalPlan plan, boolean needProjection) {
        return plan;
    }
}
