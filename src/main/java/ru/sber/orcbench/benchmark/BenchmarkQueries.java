package ru.sber.orcbench.benchmark;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ru.sber.orcbench.config.BenchmarkScenario;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;

/**
 * Spark DataFrame queries for doc / audei / st suites on AUDEI audit ORC.
 */
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
                return df.select("epk_id", "event_id", "event_ts", "name", "channel_type");
            case PARTITION_PRUNE:
                return dayFilter(df, ctx);
            case FILTER_LOW_CARDINALITY:
                return df.filter(
                        col("channel_type").equalTo(lit(ctx.channelType()))
                                .and(col("state").equalTo(lit(ctx.state())))
                );
            case FILTER_MEDIUM_CARDINALITY:
                return df.filter(
                        col("module").equalTo(lit(ctx.module()))
                                .or(col("name").equalTo(lit(ctx.name())))
                );
            case FILTER_HIGH_CARDINALITY:
                return df.filter(
                        col("epk_id").equalTo(lit(ctx.epkId()))
                                .or(col("event_id").equalTo(lit(ctx.eventId())))
                );
            case FILTER_IN:
                return df.filter(col("event_id").isin(ctx.eventIdInList().toArray())
                        .or(col("epk_id").isin(ctx.epkIdInList().toArray())));
            case FILTER_TIMESTAMP_RANGE:
                return timeRange(df, ctx.timestampStart(), ctx.timestampEnd());
            case FILTER_LOG_FORMAT:
                return df.filter(col("name").equalTo(lit(ctx.name())));
            case FILTER_COMBINED:
                return timeRange(df, ctx.timestampStart(), ctx.timestampEnd())
                        .filter(col("name").equalTo(lit(ctx.name()))
                                .and(col("state").equalTo(lit(ctx.state()))));
            case GROUP_BY:
                return df.groupBy("name", "channel_type", "state")
                        .agg(count(col("event_id")).alias("cnt"));
            case GROUP_BY_HEAVY:
                return df.filter(
                                col("event_year").equalTo(lit(ctx.eventYear()))
                                        .and(col("event_month").equalTo(lit(ctx.eventMonth())))
                        )
                        .groupBy("module", "name", "channel_type")
                        .agg(count(col("event_id")).alias("cnt"));
            case JOIN_DICTIONARY:
                return joinDictionary(df, ctx, spark, dictionaryPath);
            case TEXT_SEARCH:
                return df.filter(col("payload_json").contains(ctx.searchToken()));

            case EPK_EQ_1D:
                return timeRange(df, ctx.window1dStart(), ctx.window1dEnd())
                        .filter(col("epk_id").equalTo(lit(ctx.epkId())));
            case EPK_EQ_14D:
                return timeRange(df, ctx.window14dStart(), ctx.window14dEnd())
                        .filter(col("epk_id").equalTo(lit(ctx.epkId())));
            case EPK_PAGE:
                return timeRange(df, ctx.window14dStart(), ctx.window14dEnd())
                        .filter(col("epk_id").equalTo(lit(ctx.epkId())))
                        .orderBy(col("event_ts"))
                        .limit(FilterContext.EPK_PAGE_LIMIT);
            case EQ_FILTERS:
                return dayFilter(df, ctx)
                        .filter(col("name").equalTo(lit(ctx.name()))
                                .and(col("channel_type").equalTo(lit(ctx.channelType())))
                                .and(col("state").equalTo(lit(ctx.state()))));
            case ORDER_BY_EPK_DAY:
                return dayFilter(df, ctx).orderBy(col("epk_id"));

            case NO_FILTER:
                return timeRange(df, ctx.window31dStart(), ctx.window31dEnd());
            case LIKE_SINGLE:
                return timeRange(df, ctx.window31dStart(), ctx.window31dEnd())
                        .filter(likeContains(ctx.likeTokens().get(0)));
            case LIKE_MULTI:
                return timeRange(df, ctx.window31dStart(), ctx.window31dEnd())
                        .filter(multiLike(ctx.likeTokens(), 0, 5));
            case LIKE_FULLTEXT:
                return timeRange(df, ctx.window31dStart(), ctx.window31dEnd())
                        .filter(multiLike(ctx.likeTokens(), 0, 10));
            case EQ:
                return timeRange(df, ctx.window31dStart(), ctx.window31dEnd())
                        .filter(col("epk_id").equalTo(lit(ctx.epkId())));
            case IN_LIST:
                return timeRange(df, ctx.window31dStart(), ctx.window31dEnd())
                        .filter(col("epk_id").isin(ctx.epkIdInList().toArray()));
            case RLIKE:
                return timeRange(df, ctx.window31dStart(), ctx.window31dEnd())
                        .filter(col("payload_json").rlike(ctx.rlikePattern()));
            default:
                throw new IllegalStateException("Unsupported scenario: " + scenario);
        }
    }

    private static Dataset<Row> dayFilter(Dataset<Row> df, FilterContext ctx) {
        return df.filter(
                col("event_year").equalTo(lit(ctx.eventYear()))
                        .and(col("event_month").equalTo(lit(ctx.eventMonth())))
                        .and(col("event_day").equalTo(lit(ctx.eventDay())))
        );
    }

    private static Dataset<Row> timeRange(Dataset<Row> df, Instant start, Instant end) {
        return df.filter(
                col("event_ts").geq(lit(Timestamp.from(start)))
                        .and(col("event_ts").lt(lit(Timestamp.from(end))))
        );
    }

    private static Column likeContains(String token) {
        return col("payload_json").contains(lit(token));
    }

    private static Column multiLike(List<String> tokens, int fromInclusive, int toExclusive) {
        int end = Math.min(toExclusive, tokens.size());
        int start = Math.max(0, fromInclusive);
        if (start >= end) {
            return likeContains(tokens.get(0));
        }
        Column predicate = likeContains(tokens.get(start));
        for (int i = start + 1; i < end; i++) {
            predicate = predicate.and(likeContains(tokens.get(i)));
        }
        return predicate;
    }

    private static Dataset<Row> joinDictionary(
            Dataset<Row> events,
            FilterContext ctx,
            SparkSession spark,
            String dictionaryPath
    ) {
        Dataset<Row> fact = dayFilter(events, ctx);
        if (spark == null || dictionaryPath == null || dictionaryPath.trim().isEmpty()) {
            return fact.groupBy("name")
                    .agg(count(col("event_id")).alias("cnt"));
        }
        Dataset<Row> dictionary = spark.read().orc(dictionaryPath);
        return fact.join(dictionary, fact.col("name").equalTo(dictionary.col("event_name")), "inner")
                .filter(dictionary.col("event_family").equalTo(lit("featured")))
                .groupBy(fact.col("name"))
                .agg(count(fact.col("event_id")).alias("cnt"));
    }
}
