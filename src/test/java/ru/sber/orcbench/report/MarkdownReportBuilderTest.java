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
            .add("runs", DataTypes.LongType, false)
            .add("avg_duration_ms", DataTypes.DoubleType, true)
            .add("p50_duration_ms", DataTypes.DoubleType, true)
            .add("p95_duration_ms", DataTypes.DoubleType, true)
            .add("min_duration_ms", DataTypes.LongType, true)
            .add("max_duration_ms", DataTypes.LongType, true)
            .add("avg_selectivity", DataTypes.DoubleType, true)
            .add("index_profile", DataTypes.StringType, true)
            .add("log_format", DataTypes.StringType, true)
            .add("passed", DataTypes.BooleanType, true)
            .add("spark_runtime", DataTypes.StringType, true)
            .add("spark_version", DataTypes.StringType, true);

    @Test
    void buildsComparisonAndRecommendations() {
        List<Row> rows = Collections.unmodifiableList(Arrays.asList(
                row("benchmark", "point_lookup", "orc", 3L, 90.0, null, SparkRuntime.SPARK31_CARBON, "3.1.1"),
                row("benchmark", "point_lookup", "carbon", 3L, 60.0, null, SparkRuntime.SPARK31_CARBON, "3.1.1"),
                row("benchmark", "point_lookup", "orc", 3L, 80.0, null, SparkRuntime.SPARK32_ORC, "3.2.1"),
                row("index_experiment", "filter_high_cardinality", "carbon", 3L, 40.0, null,
                        SparkRuntime.SPARK31_CARBON, "3.1.1"),
                row("validation", "row_count_parity", "n/a", 1L, null, true, SparkRuntime.SPARK31_CARBON, "3.1.1")
        ));

        String markdown = MarkdownReportBuilder.build(rows, "test-report");

        assertTrue(markdown.contains("# test-report"));
        assertTrue(markdown.contains("ORC vs Carbon on Spark 3.1.1"));
        assertTrue(markdown.contains("ORC Spark 3.1.1 vs ORC Spark 3.2"));
        assertTrue(markdown.contains("point_lookup"));
        assertTrue(markdown.contains("Index Experiments (Bloom / Lucene)"));
        assertTrue(markdown.contains("Validation"));
        assertTrue(markdown.contains("Recommendations"));
        assertTrue(markdown.contains("CarbonData быстрее ORC"));
        assertTrue(markdown.contains("spark31-carbon"));
        assertTrue(markdown.contains("spark32-orc"));
    }

    private static Row row(
            String source,
            String scenario,
            String format,
            long runs,
            Double p50,
            Boolean passed,
            String sparkRuntime,
            String sparkVersion
    ) {
        return new GenericRowWithSchema(new Object[]{
                source, scenario, format, runs, p50, p50, p50, 1L, 2L, 0.01, null, null, passed,
                sparkRuntime, sparkVersion
        }, SCHEMA);
    }
}
