package ru.sber.orcbench.generator;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.element_at;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.pmod;
import static org.apache.spark.sql.functions.pow;
import static org.apache.spark.sql.functions.rand;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.sha2;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.when;

public final class DataGenerator {
    private static final String[] COUNTRY_CODES = {
            "RU", "US", "DE", "FR", "GB", "CN", "JP", "IN", "BR", "CA",
            "AU", "IT", "ES", "NL", "SE", "NO", "FI", "PL", "TR", "MX",
            "KR", "SG", "AE", "SA", "ZA", "AR", "CL", "CO", "ID", "MY",
            "TH", "VN", "PH", "NZ", "CH", "AT", "BE", "DK", "IE", "PT",
            "CZ", "RO", "HU", "GR", "IL", "EG", "NG", "KE", "PK", "BD"
    };
    private static final String[] DEVICE_TYPES = {"mobile", "desktop", "tablet", "tv", "iot"};
    private static final String[] STATUSES = {"success", "failed", "pending", "timeout"};

    private DataGenerator() {
    }

    public static void registerUdfs(SparkSession spark) {
        spark.udf().register(
                "build_log_message",
                (String logFormat, String eventId, Long userId, String sessionId, String countryCode,
                 String deviceType, String status, Long productId, Long campaignId, Integer regionId,
                 Long timestampMs, Double amount) ->
                        LogMessageBuilder.build(
                                logFormat, eventId, userId, sessionId, countryCode, deviceType, status,
                                productId, campaignId, regionId, timestampMs, amount
                        ),
                DataTypes.StringType
        );
    }

    public static Dataset<Row> generateChunk(
            SparkSession spark,
            GeneratorConfig config,
            int chunkIndex,
            long rowsInChunk,
            long globalRowOffset,
            long chunkStartMs,
            long chunkEndMs
    ) {
        registerUdfs(spark);

        long chunkSpanMs = Math.max(1L, chunkEndMs - chunkStartMs);
        int partitions = Math.max(1, (int) Math.min(rowsInChunk, 2000L));

        Dataset<Row> base = spark.range(0, rowsInChunk, 1, partitions)
                .withColumnRenamed("id", "row_idx")
                .withColumn("global_id", col("row_idx").plus(lit(globalRowOffset)))
                .withColumn("event_id", concat(lit("evt-"), sha2(col("global_id").cast(DataTypes.StringType), 256)))
                .withColumn("user_id", pmod(
                        col("global_id").multiply(lit(1_103_515_245L)).plus(lit(config.seed())),
                        lit(50_000_000L)
                ))
                .withColumn("session_id", concat(
                        lit("sess-"),
                        pmod(col("global_id").multiply(lit(12_345L)).plus(lit(config.seed())), lit(100_000_000L))
                ))
                .withColumn("country_code", elementAtDictionary(COUNTRY_CODES, col("global_id"), config.seed()))
                .withColumn("device_type", elementAtDictionary(DEVICE_TYPES, col("global_id"), config.seed() + 1))
                .withColumn("status", elementAtDictionary(STATUSES, col("global_id"), config.seed() + 2))
                .withColumn("product_id", mediumCardinalityId(col("global_id"), config.seed(), 100_000L))
                .withColumn("campaign_id", mediumCardinalityId(col("global_id"), config.seed() + 7, 50_000L))
                .withColumn("region_id", mediumCardinalityId(col("global_id"), config.seed() + 13, 10_000L).cast(DataTypes.IntegerType))
                .withColumn(
                        "timestamp",
                        expr("timestamp_millis(" + chunkStartMs + " + cast(row_idx * " + chunkSpanMs
                                + " / " + rowsInChunk + " as bigint))")
                )
                .withColumn("timestamp_ms", expr("cast(unix_timestamp(timestamp) * 1000 as bigint)"))
                .withColumn("amount", round(rand(config.seed() + chunkIndex).multiply(lit(10_000.0)), 2))
                .withColumn("log_format", elementAtDictionary(LogFormatType.ALL_VALUES.toArray(new String[0]), col("global_id"), config.seed() + 19))
                .withColumn(
                        "payload_json",
                        expr("build_log_message('json', event_id, user_id, session_id, country_code, device_type, "
                                + "status, product_id, campaign_id, region_id, timestamp_ms, amount)")
                )
                .withColumn(
                        "log_message",
                        expr("build_log_message(log_format, event_id, user_id, session_id, country_code, device_type, "
                                + "status, product_id, campaign_id, region_id, timestamp_ms, amount)")
                )
                .withColumn("event_year", expr("year(timestamp)"))
                .withColumn("event_month", expr("month(timestamp)"))
                .withColumn("event_day", expr("dayofmonth(timestamp)"))
                .drop("row_idx", "timestamp_ms");

        return base.select(
                "event_id",
                "user_id",
                "session_id",
                "country_code",
                "device_type",
                "status",
                "product_id",
                "campaign_id",
                "region_id",
                "timestamp",
                "amount",
                "payload_json",
                "log_format",
                "log_message",
                "event_year",
                "event_month",
                "event_day"
        );
    }

    private static Column elementAtDictionary(String[] values, Column idColumn, long seed) {
        Column[] literals = new Column[values.length];
        for (int i = 0; i < values.length; i++) {
            literals[i] = lit(values[i]);
        }
        return element_at(array(literals), pmod(idColumn.plus(lit(seed)), lit(values.length)).plus(lit(1)));
    }

    private static Column mediumCardinalityId(Column idColumn, long seed, long cardinality) {
        Column skewed = pow(rand(seed), lit(2.0)).multiply(lit(cardinality - 1));
        Column uniform = pmod(idColumn.multiply(lit(1_009_491L)).plus(lit(seed)), lit(cardinality));
        return when(rand(seed + 3).gt(0.3), skewed.cast(DataTypes.LongType)).otherwise(uniform);
    }
}
