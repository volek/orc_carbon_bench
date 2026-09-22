package ru.sber.orcbench.generator;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.element_at;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.pmod;
import static org.apache.spark.sql.functions.rand;
import static org.apache.spark.sql.functions.sha2;
import static org.apache.spark.sql.functions.when;

/**
 * Generates AUDEI-shaped audit events for ORC factor benchmarks.
 */
public final class DataGenerator {
    private static final String[] CHANNEL_TYPES = {
            "WEB_SBOL", "MP_SBOL", "ATM", "BRANCH", "CALL_CENTER", "API", "OTHER"
    };
    private static final String[] STATES = {"success", "failed", "pending", "timeout"};
    private static final String[] MODULES = {
            "CI02001608_sm_uko", "CI02001608_sm_esa", "CI02001608_sm_launcher",
            "CI02001608_sm_find", "CI02001608_sm_logon", "CI02001608_sm_audit"
    };

    private DataGenerator() {
    }

    public static void registerUdfs(SparkSession spark) {
        spark.udf().register(
                "build_audit_payload",
                (String epkId, String eventId, String session, String module, String name,
                 String channelType, String state, String userLogin, Long eventTsMs, Integer targetBytes) ->
                        AuditPayloadBuilder.build(
                                epkId, eventId, session, module, name, channelType, state, userLogin,
                                eventTsMs == null ? 0L : eventTsMs,
                                targetBytes == null ? 1600 : targetBytes
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
        int payloadTarget = (int) Math.min(Math.max(config.avgRowBytes(), 256L), 8_000L);

        Dataset<Row> base = spark.range(0, rowsInChunk, 1, partitions)
                .withColumnRenamed("id", "row_idx")
                .withColumn("global_id", col("row_idx").plus(lit(globalRowOffset)))
                .withColumn("event_id", concat(lit("evt-"), sha2(col("global_id").cast(DataTypes.StringType), 256)))
                .withColumn(
                        "epk_id",
                        expr("concat("
                                + "substr(sha2(cast(global_id as string), 256), 1, 8), '-',"
                                + "substr(sha2(cast(global_id as string), 256), 9, 4), '-',"
                                + "'4', substr(sha2(cast(global_id + " + config.seed() + " as string), 256), 14, 3), '-',"
                                + "substr(sha2(cast(global_id as string), 256), 17, 4), '-',"
                                + "substr(sha2(cast(global_id as string), 256), 21, 12)"
                                + ")")
                )
                .withColumn("session", concat(
                        lit("sess-"),
                        pmod(col("global_id").multiply(lit(12_345L)).plus(lit(config.seed())), lit(100_000_000L))
                ))
                .withColumn("module", elementAtDictionary(MODULES, col("global_id"), config.seed() + 3))
                .withColumn("name", weightedEventName(col("global_id"), config.seed()))
                .withColumn("channel_type", elementAtDictionary(CHANNEL_TYPES, col("global_id"), config.seed() + 5))
                .withColumn("state", elementAtDictionary(STATES, col("global_id"), config.seed() + 7))
                .withColumn("user_login", concat(
                        lit("user_"),
                        pmod(col("global_id").multiply(lit(97_331L)).plus(lit(config.seed())), lit(5_000_000L))
                ))
                .withColumn(
                        "event_ts",
                        expr("timestamp_millis(" + chunkStartMs + " + cast(row_idx * " + chunkSpanMs
                                + " / " + rowsInChunk + " as bigint))")
                )
                .withColumn("event_ts_ms", expr("cast(unix_timestamp(event_ts) * 1000 as bigint)"))
                .withColumn(
                        "payload_json",
                        expr("build_audit_payload(epk_id, event_id, session, module, name, channel_type, state, "
                                + "user_login, event_ts_ms, " + payloadTarget + ")")
                )
                .withColumn("event_year", expr("year(event_ts)"))
                .withColumn("event_month", expr("month(event_ts)"))
                .withColumn("event_day", expr("dayofmonth(event_ts)"))
                .withColumn("event_hour", expr("hour(event_ts)"))
                .drop("row_idx", "event_ts_ms", "global_id");

        return base.select(
                "epk_id",
                "event_id",
                "event_ts",
                "name",
                "channel_type",
                "state",
                "module",
                "session",
                "user_login",
                "payload_json",
                "event_year",
                "event_month",
                "event_day",
                "event_hour"
        );
    }

    /**
     * Approximate AUDEI daily mix: LAUNCHER 41%, ESA 39%, FIND 10%, LOGON 10%.
     */
    private static Column weightedEventName(Column idColumn, long seed) {
        Column r = pmod(idColumn.multiply(lit(1_103_515_245L)).plus(lit(seed)), lit(10_000L));
        return when(r.lt(lit(4100)), lit("LAUNCHER"))
                .when(r.lt(lit(8000)), lit("ESA"))
                .when(r.lt(lit(9000)), lit("FIND"))
                .otherwise(lit("LOGON"));
    }

    private static Column elementAtDictionary(String[] values, Column idColumn, long seed) {
        Column[] literals = new Column[values.length];
        for (int i = 0; i < values.length; i++) {
            literals[i] = lit(values[i]);
        }
        return element_at(
                array(literals),
                pmod(idColumn.plus(lit(seed)), lit(values.length)).plus(lit(1)).cast(DataTypes.IntegerType)
        );
    }
}
