package ru.sber.orcbench.validation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sber.orcbench.benchmark.DatasetLoader;
import ru.sber.orcbench.config.OrcWriteSettings;
import ru.sber.orcbench.config.SparkRuntimeInfo;
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

public final class ValidationRunner {
    private static final Logger LOG = LoggerFactory.getLogger(ValidationRunner.class);

    private ValidationRunner() {
    }

    public static void run(
            SparkSession spark,
            ValidationSettings settings,
            OrcWriteSettings orcWrite,
            String orcPath,
            String reportsValidationPath,
            long seed,
            long timestampStartMs,
            long timestampEndMs,
            SparkRuntimeInfo runtime
    ) {
        String runId = UUID.randomUUID().toString();
        List<ValidationResult> results = new ArrayList<>();

        LOG.info(
                "Validation runId={} checks={} sampleFraction={} orcPath={}",
                runId, settings.checks(), settings.sampleFraction(), orcPath
        );

        Dataset<Row> orcFull = DatasetLoader.load(spark, orcPath);
        long orcRows = orcFull.count();
        Dataset<Row> orcSample = orcFull.sample(false, settings.sampleFraction(), seed);

        for (ValidationCheck check : settings.checks()) {
            ValidationResult result;
            switch (check) {
                case ROW_COUNT:
                    result = checkRowCount(runId, orcRows, runtime);
                    break;
                case LOW_CARDINALITY_BOUNDS:
                    result = checkLowCardinalityBounds(runId, orcSample, runtime);
                    break;
                case TIMESTAMP_RANGE:
                    result = checkTimestampRange(runId, orcSample, timestampStartMs, timestampEndMs, runtime);
                    break;
                case LOG_FORMAT_DISTRIBUTION:
                    result = checkLogFormatDistribution(runId, orcSample, settings, runtime);
                    break;
                case LOG_MESSAGE_STRUCTURE:
                    result = checkLogMessageStructure(runId, orcSample, runtime);
                    break;
                case ORC_BLOOM_FILTERS:
                    result = checkOrcBloomFilters(runId, spark, orcPath, orcWrite, runtime);
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

    private static ValidationResult checkRowCount(String runId, long orcRows, SparkRuntimeInfo runtime) {
        boolean passed = orcRows > 0;
        String details = "orcRows=" + orcRows;
        return passed
                ? ValidationResult.pass(runId, ValidationCheck.ROW_COUNT, "ORC dataset is non-empty", details, runtime)
                : ValidationResult.fail(runId, ValidationCheck.ROW_COUNT, "ORC dataset is empty", details, runtime);
    }

    private static ValidationResult checkLowCardinalityBounds(
            String runId, Dataset<Row> sample, SparkRuntimeInfo runtime
    ) {
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
                ? ValidationResult.pass(runId, ValidationCheck.LOW_CARDINALITY_BOUNDS, "Low cardinality within bounds", details, runtime)
                : ValidationResult.fail(runId, ValidationCheck.LOW_CARDINALITY_BOUNDS, "Low cardinality out of bounds", details, runtime);
    }

    private static ValidationResult checkTimestampRange(
            String runId,
            Dataset<Row> sample,
            long timestampStartMs,
            long timestampEndMs,
            SparkRuntimeInfo runtime
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
                ? ValidationResult.pass(runId, ValidationCheck.TIMESTAMP_RANGE, "Timestamp range valid", details, runtime)
                : ValidationResult.fail(runId, ValidationCheck.TIMESTAMP_RANGE, "Timestamp range invalid", details, runtime);
    }

    private static ValidationResult checkLogFormatDistribution(
            String runId,
            Dataset<Row> sample,
            ValidationSettings settings,
            SparkRuntimeInfo runtime
    ) {
        List<Row> rows = sample.groupBy(col("log_format")).agg(count(lit(1)).alias("cnt")).collectAsList();
        if (rows.isEmpty()) {
            return ValidationResult.fail(runId, ValidationCheck.LOG_FORMAT_DISTRIBUTION, "No log_format values", "", runtime);
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
                ? ValidationResult.pass(runId, ValidationCheck.LOG_FORMAT_DISTRIBUTION, "log_format distribution valid", details, runtime)
                : ValidationResult.fail(runId, ValidationCheck.LOG_FORMAT_DISTRIBUTION, "log_format distribution invalid", details, runtime);
    }

    private static ValidationResult checkLogMessageStructure(
            String runId, Dataset<Row> sample, SparkRuntimeInfo runtime
    ) {
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
                ? ValidationResult.pass(runId, ValidationCheck.LOG_MESSAGE_STRUCTURE, "Log messages valid", details, runtime)
                : ValidationResult.fail(runId, ValidationCheck.LOG_MESSAGE_STRUCTURE, "Log messages invalid", details, runtime);
    }

    private static ValidationResult checkOrcBloomFilters(
            String runId,
            SparkSession spark,
            String orcPath,
            OrcWriteSettings orcWrite,
            SparkRuntimeInfo runtime
    ) {
        String[] columnsToCheck = orcWrite.bloomFiltersEnabled()
                ? orcWrite.bloomFilterColumns()
                : OrcWriteSettings.DEFAULT_BLOOM_FILTER_COLUMNS;
        boolean expectPresent = orcWrite.bloomFiltersEnabled();

        try {
            OrcMetadataInspector.InspectionResult inspection = OrcMetadataInspector.inspectBloomFilters(
                    spark.sparkContext().hadoopConfiguration(),
                    orcPath,
                    columnsToCheck,
                    expectPresent
            );
            if (inspection.passed()) {
                String message = expectPresent
                        ? "ORC bloom filters present on configured columns"
                        : "ORC bloom filters absent as expected";
                return ValidationResult.pass(runId, ValidationCheck.ORC_BLOOM_FILTERS, message, inspection.details(), runtime);
            }
            return ValidationResult.fail(
                    runId,
                    ValidationCheck.ORC_BLOOM_FILTERS,
                    "ORC bloom filter metadata check failed",
                    inspection.details(),
                    runtime
            );
        } catch (Exception ex) {
            return ValidationResult.fail(
                    runId,
                    ValidationCheck.ORC_BLOOM_FILTERS,
                    "Failed to inspect ORC bloom metadata: " + ex.getMessage(),
                    ex.toString(),
                    runtime
            );
        }
    }

    private static void writeResults(SparkSession spark, List<ValidationResult> results, String outputPath) {
        org.apache.spark.sql.types.StructType schema = new org.apache.spark.sql.types.StructType()
                .add("run_id", org.apache.spark.sql.types.DataTypes.StringType, false)
                .add("check", org.apache.spark.sql.types.DataTypes.StringType, false)
                .add("passed", org.apache.spark.sql.types.DataTypes.BooleanType, false)
                .add("message", org.apache.spark.sql.types.DataTypes.StringType, false)
                .add("details", org.apache.spark.sql.types.DataTypes.StringType, false)
                .add("executed_at", org.apache.spark.sql.types.DataTypes.StringType, false)
                .add("spark_version", org.apache.spark.sql.types.DataTypes.StringType, false)
                .add("spark_runtime", org.apache.spark.sql.types.DataTypes.StringType, false);

        List<Row> rows = results.stream()
                .map(result -> org.apache.spark.sql.RowFactory.create(
                        result.runId(),
                        result.check().cliValue(),
                        result.passed(),
                        result.message(),
                        result.details(),
                        result.executedAt().toString(),
                        result.sparkVersion(),
                        result.sparkRuntime()
                ))
                .collect(Collectors.toList());

        spark.createDataFrame(rows, schema)
                .coalesce(1)
                .write()
                .mode("overwrite")
                .parquet(outputPath);
    }
}
