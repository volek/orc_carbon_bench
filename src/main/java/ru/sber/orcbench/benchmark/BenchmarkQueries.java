package ru.sber.orcbench.benchmark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sber.orcbench.config.BenchmarkScenario;

import java.sql.Timestamp;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;

public final class BenchmarkQueries {
    private BenchmarkQueries() {
    }

    public static Dataset<Row> apply(Dataset<Row> df, BenchmarkScenario scenario, FilterContext ctx) {
        return switch (scenario) {
            case FULL_SCAN -> df;
            case PROJECTION -> df.select("event_id", "user_id", "timestamp", "amount", "log_format");
            case FILTER_LOW_CARDINALITY -> df.filter(
                    col("country_code").equalTo(lit(ctx.countryCode()))
                            .and(col("status").equalTo(lit(ctx.status())))
            );
            case FILTER_MEDIUM_CARDINALITY -> df.filter(
                    col("product_id").equalTo(lit(ctx.productId()))
                            .or(col("campaign_id").equalTo(lit(ctx.campaignId())))
            );
            case FILTER_HIGH_CARDINALITY -> df.filter(
                    col("event_id").equalTo(lit(ctx.eventId()))
                            .or(col("user_id").equalTo(lit(ctx.userId())))
            );
            case FILTER_TIMESTAMP_RANGE -> df.filter(
                    col("timestamp").geq(lit(Timestamp.from(ctx.timestampStart())))
                            .and(col("timestamp").lt(lit(Timestamp.from(ctx.timestampEnd()))))
            );
            case FILTER_LOG_FORMAT -> df.filter(col("log_format").equalTo(lit(ctx.logFormat())));
            case FILTER_COMBINED -> df.filter(
                    col("timestamp").geq(lit(Timestamp.from(ctx.timestampStart())))
                            .and(col("timestamp").lt(lit(Timestamp.from(ctx.timestampEnd()))))
                            .and(col("log_format").equalTo(lit(ctx.logFormat())))
                            .and(col("status").equalTo(lit(ctx.status())))
            );
            case GROUP_BY -> df.groupBy("country_code", "device_type", "status")
                    .agg(count(col("event_id")).alias("cnt"));
            case LUCENE_TEXT_SEARCH -> df.filter(col("log_message").contains(ctx.searchToken()));
        };
    }

    public static Dataset<Row> luceneTextSearchByLogFormat(Dataset<Row> df, FilterContext ctx, String logFormat) {
        return df.filter(
                col("log_format").equalTo(lit(logFormat))
                        .and(col("log_message").contains(ctx.searchToken()))
        );
    }
}
