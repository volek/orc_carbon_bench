package ru.sber.orcbench.benchmark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ru.sber.orcbench.config.BenchmarkScenario;

import java.sql.Timestamp;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;

public final class BenchmarkQueries {
    private BenchmarkQueries() {
    }

    public static Dataset<Row> apply(Dataset<Row> df, BenchmarkScenario scenario, FilterContext ctx) {
        return apply(df, scenario, ctx, null, null);
    }

    public static Dataset<Row> apply(
            Dataset<Row> df,
            BenchmarkScenario scenario,
            FilterContext ctx,
            SparkSession spark,
            String dictionaryPath
    ) {
        switch (scenario) {
            case FULL_SCAN:
                return df;
            case PROJECTION:
                return df.select("event_id", "user_id", "timestamp", "amount", "log_format");
            case PARTITION_PRUNE:
                return df.filter(
                        col("event_year").equalTo(lit(ctx.eventYear()))
                                .and(col("event_month").equalTo(lit(ctx.eventMonth())))
                                .and(col("event_day").equalTo(lit(ctx.eventDay())))
                );
            case FILTER_LOW_CARDINALITY:
                return df.filter(
                        col("country_code").equalTo(lit(ctx.countryCode()))
                                .and(col("status").equalTo(lit(ctx.status())))
                );
            case FILTER_MEDIUM_CARDINALITY:
                return df.filter(
                        col("product_id").equalTo(lit(ctx.productId()))
                                .or(col("campaign_id").equalTo(lit(ctx.campaignId())))
                );
            case FILTER_HIGH_CARDINALITY:
                return df.filter(
                        col("event_id").equalTo(lit(ctx.eventId()))
                                .or(col("user_id").equalTo(lit(ctx.userId())))
                );
            case FILTER_IN:
                return df.filter(col("event_id").isin(ctx.eventIdInList().toArray())
                        .or(col("product_id").isin(toObjectArray(ctx.productIdInList()))));
            case FILTER_TIMESTAMP_RANGE:
                return df.filter(
                        col("timestamp").geq(lit(Timestamp.from(ctx.timestampStart())))
                                .and(col("timestamp").lt(lit(Timestamp.from(ctx.timestampEnd()))))
                );
            case FILTER_LOG_FORMAT:
                return df.filter(col("log_format").equalTo(lit(ctx.logFormat())));
            case FILTER_COMBINED:
                return df.filter(
                        col("timestamp").geq(lit(Timestamp.from(ctx.timestampStart())))
                                .and(col("timestamp").lt(lit(Timestamp.from(ctx.timestampEnd()))))
                                .and(col("log_format").equalTo(lit(ctx.logFormat())))
                                .and(col("status").equalTo(lit(ctx.status())))
                );
            case GROUP_BY:
                return df.groupBy("country_code", "device_type", "status")
                        .agg(count(col("event_id")).alias("cnt"));
            case GROUP_BY_HEAVY:
                return df.filter(
                                col("event_year").equalTo(lit(ctx.eventYear()))
                                        .and(col("event_month").equalTo(lit(ctx.eventMonth())))
                        )
                        .groupBy("product_id", "campaign_id", "country_code")
                        .agg(count(col("event_id")).alias("cnt"));
            case JOIN_DICTIONARY:
                return joinDictionary(df, ctx, spark, dictionaryPath);
            case TEXT_SEARCH:
                return df.filter(col("log_message").contains(ctx.searchToken()));
            default:
                throw new IllegalStateException("Unsupported scenario: " + scenario);
        }
    }

    private static Dataset<Row> joinDictionary(
            Dataset<Row> events,
            FilterContext ctx,
            SparkSession spark,
            String dictionaryPath
    ) {
        Dataset<Row> fact = events.filter(
                col("event_year").equalTo(lit(ctx.eventYear()))
                        .and(col("event_month").equalTo(lit(ctx.eventMonth())))
                        .and(col("event_day").equalTo(lit(ctx.eventDay())))
        );
        if (spark == null || dictionaryPath == null || dictionaryPath.trim().isEmpty()) {
            // Fallback without external dictionary: self-join style aggregation on medium card.
            return fact.groupBy("product_id")
                    .agg(count(col("event_id")).alias("cnt"));
        }
        Dataset<Row> dictionary = spark.read().orc(dictionaryPath);
        return fact.join(dictionary, fact.col("product_id").equalTo(dictionary.col("product_id")), "inner")
                .filter(dictionary.col("product_type").equalTo(lit("featured")))
                .groupBy(fact.col("product_id"))
                .agg(count(fact.col("event_id")).alias("cnt"));
    }

    private static Object[] toObjectArray(List<Long> values) {
        Object[] array = new Object[values.size()];
        for (int i = 0; i < values.size(); i++) {
            array[i] = values.get(i);
        }
        return array;
    }
}
