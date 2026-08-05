package ru.sber.orcbench.validation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sber.orcbench.benchmark.DatasetLoader;
import ru.sber.orcbench.benchmark.FilterContext;
import ru.sber.orcbench.config.OutputFormat;
import ru.sber.orcbench.config.ValidationCheck;
import ru.sber.orcbench.config.ValidationSettings;
import ru.sber.orcbench.generator.LogFormatType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.length;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.not;
import static org.apache.spark.sql.functions.sum;

public final class ValidationRunner {
    private static final Logger LOG = LoggerFactory.getLogger(ValidationRunner.class);

    private ValidationRunner() {
    }

    public static void run(
            SparkSession spark,
            ValidationSettings settings,
            String orcPath,
            String carbonPath,
            String reportsValidationPath,
            long seed,
            long timestampStartMs,
            long timestampEndMs
    ) {
        String runId = UUID.randomUUID().toString();
        List<ValidationResult> results = new ArrayList<>();

        LOG.info(
                "Validation runId={} checks={} sampleFraction={} orcPath={} carbonPath={}",
                runId, settings.checks(), settings.sampleFraction(), orcPath, carbonPath
        );

        Dataset<Row> orcFull = DatasetLoader.load(spark, OutputFormat.ORC, orcPath, carbonPath);
        Dataset<Row> carbonFull = DatasetLoader.load(spark, OutputFormat.CARBON, orcPath, carbonPath);

        long orcRows = orcFull.count();
        long carbonRows = carbonFull.count();

        Dataset<Row> orcSample = orcFull.sample(false, settings.sampleFraction(), seed);
        Dataset<Row> carbonSample = carbonFull.sample(false, settings.sampleFraction(), seed);

        for (ValidationCheck check : settings.checks()) {
            ValidationResult result;
            switch (check) {
                case ROW_COUNT_PARITY:
                    result = checkRowCountParity(runId, orcRows, carbonRows);
                    break;
                case CHECKSUM_PARITY:
                    result = checkChecksumParity(runId, orcFull, carbonFull);
                    break;
                case SAMPLE_QUERY_PARITY:
                    result = checkSampleQueryParity(
                            runId, orcFull, carbonFull, timestampStartMs, timestampEndMs
                    );
                    break;
                case LOW_CARDINALITY_BOUNDS:
                    result = checkLowCardinalityBounds(runId, orcSample);
                    break;
                case TIMESTAMP_RANGE:
                    result = checkTimestampRange(runId, orcSample, timestampStartMs, timestampEndMs);
                    break;
                case LOG_FORMAT_DISTRIBUTION:
                    result = checkLogFormatDistribution(runId, orcSample, settings);
                    break;
                case LOG_MESSAGE_STRUCTURE:
                    result = checkLogMessageStructure(runId, orcSample);
                    break;
                default:
                    throw new IllegalStateException("Unsupported validation check: " + check);
            }
            results.add(result);
            LOG.info("Validation check={} passed={} message={}", check.cliValue(), result.passed(), result.message());
        }

        writeResults(spark, results, reportsValidationPath);

        long failed = results.stream().filter(result -> !result.passed()).count();
        if (failed > 0) {
            throw new IllegalStateException("Validation failed: " + failed + " of " + results.size() + " checks failed");
        }

        LOG.info("Validation completed successfully: runId={} checks={}", runId, results.size());
    }

    private static ValidationResult checkRowCountParity(String runId, long orcRows, long carbonRows) {
        boolean passed = orcRows == carbonRows;
        String details = "orcRows=" + orcRows + ", carbonRows=" + carbonRows;
        return passed
                ? ValidationResult.pass(runId, ValidationCheck.ROW_COUNT_PARITY, "Row counts match", details)
                : ValidationResult.fail(runId, ValidationCheck.ROW_COUNT_PARITY, "Row counts differ", details);
    }

    private static ValidationResult checkChecksumParity(String runId, Dataset<Row> orc, Dataset<Row> carbon) {
        long orcChecksum = aggregateChecksum(orc);
        long carbonChecksum = aggregateChecksum(carbon);
        boolean passed = orcChecksum == carbonChecksum;
        String details = "orcChecksum=" + orcChecksum + ", carbonChecksum=" + carbonChecksum;
        return passed
                ? ValidationResult.pass(runId, ValidationCheck.CHECKSUM_PARITY, "Checksums match", details)
                : ValidationResult.fail(runId, ValidationCheck.CHECKSUM_PARITY, "Checksums differ", details);
    }

    private static long aggregateChecksum(Dataset<Row> df) {
        Row row = df.agg(
                sum(col("user_id")).alias("user_sum"),
                sum(col("product_id")).alias("product_sum"),
                sum(col("campaign_id")).alias("campaign_sum"),
                sum(length(col("event_id"))).alias("event_len_sum")
        ).collectAsList().get(0);

        long userSum = row.isNullAt(0) ? 0L : row.getLong(0);
        long productSum = row.isNullAt(1) ? 0L : row.getLong(1);
        long campaignSum = row.isNullAt(2) ? 0L : row.getLong(2);
        long eventLenSum = row.isNullAt(3) ? 0L : row.getLong(3);
        return userSum ^ productSum ^ campaignSum ^ eventLenSum;
    }

    private static ValidationResult checkSampleQueryParity(
            String runId,
            Dataset<Row> orc,
            Dataset<Row> carbon,
            long timestampStartMs,
            long timestampEndMs
    ) {
        List<Row> sample = orc.limit(1).collectAsList();
        if (sample.isEmpty()) {
            return ValidationResult.fail(runId, ValidationCheck.SAMPLE_QUERY_PARITY, "Empty ORC dataset", "");
        }

        FilterContext ctx = FilterContext.fromSample(sample.get(0), timestampStartMs, timestampEndMs);

        long orcCount = orc.filter(
                col("country_code").equalTo(lit(ctx.countryCode()))
                        .and(col("status").equalTo(lit(ctx.status())))
                        .and(col("log_format").equalTo(lit(ctx.logFormat())))
        ).count();

        long carbonCount = carbon.filter(
                col("country_code").equalTo(lit(ctx.countryCode()))
                        .and(col("status").equalTo(lit(ctx.status())))
                        .and(col("log_format").equalTo(lit(ctx.logFormat())))
        ).count();

        boolean passed = orcCount == carbonCount;
        String details = "filter country=" + ctx.countryCode() + " status=" + ctx.status()
                + " log_format=" + ctx.logFormat() + " orcCount=" + orcCount + " carbonCount=" + carbonCount;
        return passed
                ? ValidationResult.pass(runId, ValidationCheck.SAMPLE_QUERY_PARITY, "Sample query counts match", details)
                : ValidationResult.fail(runId, ValidationCheck.SAMPLE_QUERY_PARITY, "Sample query counts differ", details);
    }

    private static ValidationResult checkLowCardinalityBounds(String runId, Dataset<Row> sample) {
        Row row = sample.agg(
                countDistinct(col("country_code")).alias("country_distinct"),
                countDistinct(col("device_type")).alias("device_distinct"),
                countDistinct(col("status")).alias("status_distinct"),
                countDistinct(col("log_format")).alias("log_format_distinct")
        ).collectAsList().get(0);

        long countryDistinct = row.getLong(0);
        long deviceDistinct = row.getLong(1);
        long statusDistinct = row.getLong(2);
        long logFormatDistinct = row.getLong(3);

        boolean passed = countryDistinct <= 50
                && deviceDistinct <= 5
                && statusDistinct <= 4
                && logFormatDistinct <= LogFormatType.ALL_VALUES.size();

        String details = "country=" + countryDistinct + " device=" + deviceDistinct
                + " status=" + statusDistinct + " log_format=" + logFormatDistinct;

        return passed
                ? ValidationResult.pass(runId, ValidationCheck.LOW_CARDINALITY_BOUNDS, "Low cardinality within bounds", details)
                : ValidationResult.fail(runId, ValidationCheck.LOW_CARDINALITY_BOUNDS, "Low cardinality out of bounds", details);
    }

    private static ValidationResult checkTimestampRange(
            String runId,
            Dataset<Row> sample,
            long timestampStartMs,
            long timestampEndMs
    ) {
        Row row = sample.agg(
                min(col("timestamp")).alias("min_ts"),
                max(col("timestamp")).alias("max_ts")
        ).collectAsList().get(0);

        long minTs = row.getTimestamp(0).getTime();
        long maxTs = row.getTimestamp(1).getTime();

        boolean passed = minTs >= timestampStartMs && maxTs < timestampEndMs;
        String details = "minTs=" + minTs + " maxTs=" + maxTs
                + " expected=[" + timestampStartMs + ".." + timestampEndMs + ")";

        return passed
                ? ValidationResult.pass(runId, ValidationCheck.TIMESTAMP_RANGE, "Timestamp range valid", details)
                : ValidationResult.fail(runId, ValidationCheck.TIMESTAMP_RANGE, "Timestamp range invalid", details);
    }

    private static ValidationResult checkLogFormatDistribution(
            String runId,
            Dataset<Row> sample,
            ValidationSettings settings
    ) {
        List<Row> rows = sample.groupBy(col("log_format")).agg(count(lit(1)).alias("cnt")).collectAsList();
        if (rows.isEmpty()) {
            return ValidationResult.fail(runId, ValidationCheck.LOG_FORMAT_DISTRIBUTION, "No log_format values", "");
        }

        long total = rows.stream().mapToLong(row -> row.getLong(1)).sum();
        double expectedShare = 1.0 / LogFormatType.ALL_VALUES.size();
        Map<String, Long> counts = rows.stream()
                .collect(Collectors.toMap(row -> row.getString(0), row -> row.getLong(1)));

        boolean allFormatsPresent = LogFormatType.ALL_VALUES.stream().allMatch(counts::containsKey);
        boolean sharesBalanced = counts.values().stream().allMatch(count -> {
            double share = (double) count / total;
            return Math.abs(share - expectedShare) <= settings.logFormatShareTolerance();
        });

        boolean passed = allFormatsPresent && sharesBalanced;
        String details = "counts=" + counts + " tolerance=" + settings.logFormatShareTolerance();

        return passed
                ? ValidationResult.pass(runId, ValidationCheck.LOG_FORMAT_DISTRIBUTION, "log_format distribution valid", details)
                : ValidationResult.fail(runId, ValidationCheck.LOG_FORMAT_DISTRIBUTION, "log_format distribution invalid", details);
    }

    private static ValidationResult checkLogMessageStructure(String runId, Dataset<Row> sample) {
        long emptyMessages = sample.filter(
                col("log_message").isNull().or(length(col("log_message")).leq(0))
        ).count();

        long jsonFormatInvalid = sample.filter(
                col("log_format").equalTo(lit("json"))
                        .and(col("log_message").isNull().or(not(col("log_message").startsWith(lit("{")))))
        ).count();

        boolean passed = emptyMessages == 0 && jsonFormatInvalid == 0;
        String details = "emptyMessages=" + emptyMessages + " jsonFormatInvalid=" + jsonFormatInvalid;

        return passed
                ? ValidationResult.pass(runId, ValidationCheck.LOG_MESSAGE_STRUCTURE, "Log messages valid", details)
                : ValidationResult.fail(runId, ValidationCheck.LOG_MESSAGE_STRUCTURE, "Log messages invalid", details);
    }

    private static void writeResults(SparkSession spark, List<ValidationResult> results, String outputPath) {
        org.apache.spark.sql.types.StructType schema = new org.apache.spark.sql.types.StructType()
                .add("run_id", org.apache.spark.sql.types.DataTypes.StringType, false)
                .add("check", org.apache.spark.sql.types.DataTypes.StringType, false)
                .add("passed", org.apache.spark.sql.types.DataTypes.BooleanType, false)
                .add("message", org.apache.spark.sql.types.DataTypes.StringType, false)
                .add("details", org.apache.spark.sql.types.DataTypes.StringType, false)
                .add("executed_at", org.apache.spark.sql.types.DataTypes.StringType, false);

        List<Row> rows = results.stream()
                .map(result -> org.apache.spark.sql.RowFactory.create(
                        result.runId(),
                        result.check().cliValue(),
                        result.passed(),
                        result.message(),
                        result.details(),
                        result.executedAt().toString()
                ))
                .collect(Collectors.toList());

        spark.createDataFrame(rows, schema)
                .coalesce(1)
                .write()
                .mode("overwrite")
                .parquet(outputPath);
    }
}
