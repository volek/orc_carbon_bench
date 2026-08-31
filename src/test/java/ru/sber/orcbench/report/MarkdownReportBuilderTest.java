package ru.sber.orcbench.report;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import ru.sber.orcbench.config.SparkRuntime;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

class MarkdownReportBuilderTest {

    private static final StructType SCHEMA = new StructType()
            .add("source", DataTypes.StringType, false)
            .add("scenario", DataTypes.StringType, false)
            .add("format", DataTypes.StringType, false)
            .add("dataset_label", DataTypes.StringType, true)
            .add("runs", DataTypes.LongType, false)
            .add("avg_duration_ms", DataTypes.DoubleType, true)
            .add("p50_duration_ms", DataTypes.DoubleType, true)
            .add("p95_duration_ms", DataTypes.DoubleType, true)
            .add("min_duration_ms", DataTypes.LongType, true)
            .add("max_duration_ms", DataTypes.LongType, true)
            .add("avg_selectivity", DataTypes.DoubleType, true)
            .add("avg_bytes_read", DataTypes.DoubleType, true)
            .add("avg_records_read", DataTypes.DoubleType, true)
            .add("orc_bloom_columns", DataTypes.StringType, true)
            .add("passed", DataTypes.BooleanType, true)
            .add("spark_runtime", DataTypes.StringType, true)
            .add("spark_version", DataTypes.StringType, true);

    @Test
    void buildsOrcBenchmarkReport() {
        List<Row> rows = Collections.unmodifiableList(Arrays.asList(
                benchmarkRow("filter_high_cardinality", "nobloom", 3L, 90.0, 1.0e9, "none"),
                benchmarkRow("full_scan", "nobloom", 3L, 120.0, 1.0e9, "none"),
                validationRow("row_count", true)
        ));

        String markdown = MarkdownReportBuilder.build(rows, "test-report");

        assertTrue(markdown.contains("# test-report"));
        assertTrue(markdown.contains("Benchmark Summary"));
        assertTrue(markdown.contains("dataset"));
        assertTrue(markdown.contains("bloom_columns"));
        assertTrue(markdown.contains("filter_high_cardinality"));
        assertTrue(markdown.contains("Validation"));
        assertTrue(markdown.contains("PASS"));
        assertTrue(markdown.contains("Recommendations"));
        assertTrue(markdown.contains("spark32-orc"));
    }

    @Test
    void buildsBloomComparisonWhenAbPresent() {
        List<Row> rows = Arrays.asList(
                benchmarkRow("filter_high_cardinality", "nobloom", 3L, 100.0, 1.0e9, "none"),
                benchmarkRow("filter_high_cardinality", "bloom", 3L, 80.0, 2.0e8,
                        "event_id,user_id,product_id,campaign_id")
        );

        String markdown = MarkdownReportBuilder.build(rows, "bloom-report");

        assertTrue(markdown.contains("Bloom filter comparison"));
        assertTrue(markdown.contains("filter_high_cardinality"));
        assertTrue(markdown.contains("Bloom A/B"));
    }

    @Test
    void formatsLongPercentileColumnsWithoutClassCast() {
        StructType longSchema = new StructType()
                .add("source", DataTypes.StringType, false)
                .add("scenario", DataTypes.StringType, false)
                .add("format", DataTypes.StringType, false)
                .add("dataset_label", DataTypes.StringType, true)
                .add("runs", DataTypes.LongType, false)
                .add("avg_duration_ms", DataTypes.DoubleType, true)
                .add("p50_duration_ms", DataTypes.LongType, true)
                .add("p95_duration_ms", DataTypes.LongType, true)
                .add("min_duration_ms", DataTypes.LongType, true)
                .add("max_duration_ms", DataTypes.LongType, true)
                .add("avg_selectivity", DataTypes.DoubleType, true)
                .add("orc_bloom_columns", DataTypes.StringType, true)
                .add("passed", DataTypes.BooleanType, true)
                .add("spark_runtime", DataTypes.StringType, true)
                .add("spark_version", DataTypes.StringType, true);

        Row row = new GenericRowWithSchema(new Object[]{
                "benchmark", "full_scan", "orc", "default", 3L, 115.5, 120L, 140L, 100L, 150L, 0.02,
                "none", null, SparkRuntime.SPARK32_ORC, "3.2.1"
        }, longSchema);

        String markdown = MarkdownReportBuilder.build(Collections.singletonList(row), "long-p50");

        assertTrue(markdown.contains("120.00"));
        assertTrue(markdown.contains("140.00"));
    }

    private static Row benchmarkRow(
            String scenario,
            String datasetLabel,
            long runs,
            Double p50,
            Double avgBytes,
            String bloomColumns
    ) {
        return new GenericRowWithSchema(new Object[]{
                "benchmark", scenario, "orc", datasetLabel, runs, p50, p50, p50, 1L, 2L, 0.01,
                avgBytes, avgBytes / 100.0, bloomColumns, null, SparkRuntime.SPARK32_ORC, "3.2.1"
        }, SCHEMA);
    }

    private static Row validationRow(String check, boolean passed) {
        return new GenericRowWithSchema(new Object[]{
                "validation", check, "orc", "-", 1L, null, null, null, null, null, null,
                null, null, null, passed, SparkRuntime.SPARK32_ORC, "3.2.1"
        }, SCHEMA);
    }
}
