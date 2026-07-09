package ru.sber.orcbench.report;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

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
            .add("passed", DataTypes.BooleanType, true);

    @Test
    void buildsComparisonAndRecommendations() {
        List<Row> rows = List.of(
                row("benchmark", "point_lookup", "orc", 3L, 90.0, null),
                row("benchmark", "point_lookup", "carbon", 3L, 60.0, null),
                row("validation", "row_count_parity", "n/a", 1L, null, true)
        );

        String markdown = MarkdownReportBuilder.build(rows, "test-report");

        assertTrue(markdown.contains("# test-report"));
        assertTrue(markdown.contains("ORC vs Carbon Comparison"));
        assertTrue(markdown.contains("point_lookup"));
        assertTrue(markdown.contains("Validation"));
        assertTrue(markdown.contains("Recommendations"));
        assertTrue(markdown.contains("CarbonData быстрее ORC"));
    }

    private static Row row(String source, String scenario, String format, long runs, Double p50, Boolean passed) {
        return new GenericRowWithSchema(new Object[]{
                source, scenario, format, runs, p50, p50, p50, 1L, 2L, 0.01, null, null, passed
        }, SCHEMA);
    }
}
