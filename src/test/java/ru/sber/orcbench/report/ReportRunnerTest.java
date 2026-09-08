package ru.sber.orcbench.report;

import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.sber.orcbench.config.ReportFormat;
import ru.sber.orcbench.config.ReportSettings;
import ru.sber.orcbench.config.SparkRuntime;
import ru.sber.orcbench.config.StoragePaths;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReportRunnerTest {
    private static SparkSession spark;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .master("local[2]")
                .appName("ReportRunnerTest")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate();
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    void buildsReportFromValidationOnly(@TempDir Path tempDir) throws Exception {
        StoragePaths paths = StoragePaths.from(tempDir.toAbsolutePath().toString(), null, null, null, null);
        writeValidation(paths.reportsValidationPath(), "row_count", true);

        ReportRunner.run(spark, reportSettings(), paths, paths.reportsSummaryPath());

        String markdown = new String(Files.readAllBytes(
                java.nio.file.Paths.get(paths.reportsSummaryPath(), "pilot-report.md")));
        assertTrue(markdown.contains("Validation"));
        assertTrue(markdown.contains("row_count"));
        assertTrue(markdown.contains("PASS"));
    }

    @Test
    void buildsReportFromLegacyValidationInRawRoot(@TempDir Path tempDir) {
        StoragePaths paths = StoragePaths.from(tempDir.toAbsolutePath().toString(), null, null, null, null);
        writeValidation(paths.reportsRawPath(), "orc_bloom_filters", true);

        ReportRunner.run(spark, reportSettings(), paths, paths.reportsSummaryPath());

        assertTrue(Files.exists(java.nio.file.Paths.get(paths.reportsSummaryPath(), "pilot-report.md")));
    }

    @Test
    void buildsReportFromBloomAbSubdirectories(@TempDir Path tempDir) throws Exception {
        StoragePaths paths = StoragePaths.from(tempDir.toAbsolutePath().toString(), null, null, null, null);
        writeBenchmark(paths.reportsBenchmarkNobloomPath(), "nobloom");
        writeBenchmark(paths.reportsBenchmarkBloomPath(), "bloom");
        writeValidation(paths.reportsValidationNobloomPath(), "row_count", true);
        writeValidation(paths.reportsValidationBloomPath(), "orc_bloom_filters", true);

        ReportRunner.run(spark, reportSettings(), paths, paths.reportsSummaryPath());

        String markdown = new String(Files.readAllBytes(
                java.nio.file.Paths.get(paths.reportsSummaryPath(), "pilot-report.md")));
        assertTrue(markdown.contains("Benchmark Summary"));
        assertTrue(markdown.contains("nobloom"));
        assertTrue(markdown.contains("bloom"));
        assertTrue(markdown.contains("Validation"));
        assertFalse(markdown.contains("_Нет данных_"));
    }

    @Test
    void failsWhenRawDirectoryIsEmpty(@TempDir Path tempDir) {
        StoragePaths paths = StoragePaths.from(tempDir.toAbsolutePath().toString(), null, null, null, null);
        IllegalStateException ex = assertThrows(
                IllegalStateException.class,
                () -> ReportRunner.run(spark, reportSettings(), paths, paths.reportsSummaryPath())
        );
        assertTrue(ex.getMessage().contains("No report input data found"));
        assertTrue(ex.getMessage().contains(paths.reportsRawPath()));
    }

    private static ReportSettings reportSettings() {
        return new ReportSettings(EnumSet.of(ReportFormat.JSON, ReportFormat.MARKDOWN), "pilot-report");
    }

    private static void writeValidation(String outputPath, String check, boolean passed) {
        StructType schema = new StructType()
                .add("run_id", DataTypes.StringType, false)
                .add("check", DataTypes.StringType, false)
                .add("passed", DataTypes.BooleanType, false)
                .add("message", DataTypes.StringType, false)
                .add("details", DataTypes.StringType, false)
                .add("executed_at", DataTypes.StringType, false)
                .add("spark_version", DataTypes.StringType, false)
                .add("spark_runtime", DataTypes.StringType, false);
        List<org.apache.spark.sql.Row> rows = Collections.singletonList(RowFactory.create(
                "run-1",
                check,
                passed,
                passed ? "ok" : "fail",
                "details",
                "2026-09-03T13:08:00Z",
                "3.2.1",
                SparkRuntime.SPARK32_ORC
        ));
        spark.createDataFrame(rows, schema).coalesce(1).write().mode("overwrite").parquet(outputPath);
    }

    private static void writeBenchmark(String outputPath, String datasetLabel) {
        StructType schema = new StructType()
                .add("run_id", DataTypes.StringType, false)
                .add("scenario", DataTypes.StringType, false)
                .add("format", DataTypes.StringType, false)
                .add("run_index", DataTypes.IntegerType, false)
                .add("warmup", DataTypes.BooleanType, false)
                .add("duration_ms", DataTypes.LongType, false)
                .add("rows_returned", DataTypes.LongType, false)
                .add("total_rows", DataTypes.LongType, false)
                .add("selectivity", DataTypes.DoubleType, false)
                .add("bytes_read", DataTypes.LongType, false)
                .add("records_read", DataTypes.LongType, false)
                .add("seed", DataTypes.LongType, false)
                .add("dataset_label", DataTypes.StringType, false)
                .add("orc_path", DataTypes.StringType, false)
                .add("orc_bloom_columns", DataTypes.StringType, false)
                .add("executed_at", DataTypes.StringType, false)
                .add("spark_version", DataTypes.StringType, false)
                .add("spark_runtime", DataTypes.StringType, false);
        List<org.apache.spark.sql.Row> rows = Collections.singletonList(RowFactory.create(
                "run-1",
                "filter_high_cardinality",
                "orc",
                0,
                false,
                1000L,
                1L,
                1000L,
                0.001d,
                12345L,
                10L,
                42L,
                datasetLabel,
                "/tmp/orc",
                "nobloom".equals(datasetLabel) ? "none" : "event_id",
                "2026-09-03T13:08:00Z",
                "3.2.1",
                SparkRuntime.SPARK32_ORC
        ));
        spark.createDataFrame(rows, schema).coalesce(1).write().mode("overwrite").parquet(outputPath);
    }
}
