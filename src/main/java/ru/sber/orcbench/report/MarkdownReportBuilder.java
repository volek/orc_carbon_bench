package ru.sber.orcbench.report;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sber.orcbench.config.SparkRuntime;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class MarkdownReportBuilder {
    private MarkdownReportBuilder() {
    }

    public static String build(Dataset<Row> summary, String reportName) {
        return build(summary.collectAsList(), reportName);
    }

    public static String build(List<Row> rows, String reportName) {
        StringBuilder md = new StringBuilder();
        md.append("# ").append(reportName).append("\n\n");
        md.append("Сводный отчёт сравнения ORC и CarbonData (Spark 3.1.1) "
                + "и референса ORC на кластерном Spark 3.2.\n\n");

        appendSection(md, "Benchmark Summary", filterSource(rows, "benchmark"), true);
        appendOrcVsCarbonComparison(md, rows);
        appendOrcEngineComparison(md, rows);
        appendSection(md, "Index Experiments (Bloom / Lucene)", filterSource(rows, "index_experiment"), true);
        appendSection(md, "Index Build Metrics", filterSource(rows, "index_build"), false);
        appendValidationSection(md, filterSource(rows, "validation"));
        appendRecommendations(md, rows);

        return md.toString();
    }

    private static List<Row> filterSource(List<Row> rows, String source) {
        return rows.stream()
                .filter(row -> source.equals(row.getString(row.fieldIndex("source"))))
                .sorted(Comparator.comparing((Row r) -> r.getString(r.fieldIndex("scenario")))
                        .thenComparing(r -> nullableString(r, "format"))
                        .thenComparing(r -> nullableString(r, "spark_runtime")))
                .collect(Collectors.toList());
    }

    private static List<Row> filterSpark31Benchmark(List<Row> rows) {
        return filterSource(rows, "benchmark").stream()
                .filter(row -> SparkRuntime.SPARK31_CARBON.equals(nullableString(row, "spark_runtime"))
                        || "-".equals(nullableString(row, "spark_runtime")))
                .collect(Collectors.toList());
    }

    private static void appendSection(StringBuilder md, String title, List<Row> rows, boolean includeFormat) {
        md.append("## ").append(title).append("\n\n");
        if (rows.isEmpty()) {
            md.append("_Нет данных_\n\n");
            return;
        }

        if (includeFormat) {
            md.append("| scenario | format | spark_runtime | runs | avg_ms | p50_ms | p95_ms | avg_selectivity |\n");
            md.append("|---|---|---|---:|---:|---:|---:|---:|\n");
            for (Row row : rows) {
                md.append("| ").append(row.getString(row.fieldIndex("scenario")))
                        .append(" | ").append(nullableString(row, "format"))
                        .append(" | ").append(nullableString(row, "spark_runtime"))
                        .append(" | ").append(row.getLong(row.fieldIndex("runs")))
                        .append(" | ").append(formatDouble(row, "avg_duration_ms"))
                        .append(" | ").append(formatDouble(row, "p50_duration_ms"))
                        .append(" | ").append(formatDouble(row, "p95_duration_ms"))
                        .append(" | ").append(formatDouble(row, "avg_selectivity"))
                        .append(" |\n");
            }
        } else {
            md.append("| scenario | format | spark_runtime | runs | avg_ms | p50_ms | p95_ms |\n");
            md.append("|---|---|---|---:|---:|---:|---:|\n");
            for (Row row : rows) {
                md.append("| ").append(row.getString(row.fieldIndex("scenario")))
                        .append(" | ").append(nullableString(row, "format"))
                        .append(" | ").append(nullableString(row, "spark_runtime"))
                        .append(" | ").append(row.getLong(row.fieldIndex("runs")))
                        .append(" | ").append(formatDouble(row, "avg_duration_ms"))
                        .append(" | ").append(formatDouble(row, "p50_duration_ms"))
                        .append(" | ").append(formatDouble(row, "p95_duration_ms"))
                        .append(" |\n");
            }
        }
        md.append("\n");
    }

    private static void appendOrcVsCarbonComparison(StringBuilder md, List<Row> rows) {
        md.append("## ORC vs Carbon on Spark 3.1.1 (p50)\n\n");

        Map<String, Map<String, Double>> byScenario = new LinkedHashMap<>();
        for (Row row : filterSpark31Benchmark(rows)) {
            String scenario = row.getString(row.fieldIndex("scenario"));
            String format = nullableString(row, "format");
            double p50 = row.isNullAt(row.fieldIndex("p50_duration_ms"))
                    ? 0.0 : row.getDouble(row.fieldIndex("p50_duration_ms"));
            byScenario.computeIfAbsent(scenario, key -> new LinkedHashMap<>()).put(format, p50);
        }

        if (byScenario.isEmpty()) {
            md.append("_Нет данных benchmark Spark 3.1.1 для сравнения форматов_\n\n");
            return;
        }

        md.append("| scenario | orc_p50_ms | carbon_p50_ms | delta_% |\n");
        md.append("|---|---:|---:|---:|\n");
        for (Map.Entry<String, Map<String, Double>> entry : byScenario.entrySet()) {
            double orc = entry.getValue().getOrDefault("orc", 0.0);
            double carbon = entry.getValue().getOrDefault("carbon", 0.0);
            double delta = orc == 0.0 ? 0.0 : ((carbon - orc) / orc) * 100.0;
            md.append("| ").append(entry.getKey())
                    .append(" | ").append(String.format("%.2f", orc))
                    .append(" | ").append(String.format("%.2f", carbon))
                    .append(" | ").append(String.format("%.1f", delta))
                    .append(" |\n");
        }
        md.append("\n");
    }

    private static void appendOrcEngineComparison(StringBuilder md, List<Row> rows) {
        md.append("## ORC Spark 3.1.1 vs ORC Spark 3.2 (p50)\n\n");

        Map<String, Map<String, Double>> byScenario = new LinkedHashMap<>();
        for (Row row : filterSource(rows, "benchmark")) {
            if (!"orc".equals(nullableString(row, "format"))) {
                continue;
            }
            String scenario = row.getString(row.fieldIndex("scenario"));
            String runtime = nullableString(row, "spark_runtime");
            double p50 = row.isNullAt(row.fieldIndex("p50_duration_ms"))
                    ? 0.0 : row.getDouble(row.fieldIndex("p50_duration_ms"));
            byScenario.computeIfAbsent(scenario, key -> new LinkedHashMap<>()).put(runtime, p50);
        }

        boolean hasSpark32 = byScenario.values().stream()
                .anyMatch(values -> values.containsKey(SparkRuntime.SPARK32_ORC));
        if (!hasSpark32) {
            md.append("_Нет референсных ORC-метрик Spark 3.2_\n\n");
            return;
        }

        md.append("| scenario | orc_spark31_p50_ms | orc_spark32_p50_ms | delta_% |\n");
        md.append("|---|---:|---:|---:|\n");
        for (Map.Entry<String, Map<String, Double>> entry : byScenario.entrySet()) {
            double spark31 = entry.getValue().getOrDefault(SparkRuntime.SPARK31_CARBON, 0.0);
            double spark32 = entry.getValue().getOrDefault(SparkRuntime.SPARK32_ORC, 0.0);
            double delta = spark31 == 0.0 ? 0.0 : ((spark32 - spark31) / spark31) * 100.0;
            md.append("| ").append(entry.getKey())
                    .append(" | ").append(String.format("%.2f", spark31))
                    .append(" | ").append(String.format("%.2f", spark32))
                    .append(" | ").append(String.format("%.1f", delta))
                    .append(" |\n");
        }
        md.append("\n");
    }

    private static void appendValidationSection(StringBuilder md, List<Row> rows) {
        md.append("## Validation\n\n");
        if (rows.isEmpty()) {
            md.append("_Нет данных валидации_\n\n");
            return;
        }
        md.append("| check | passed |\n");
        md.append("|---|---|\n");
        for (Row row : rows) {
            md.append("| ").append(row.getString(row.fieldIndex("scenario")))
                    .append(" | ").append(row.getBoolean(row.fieldIndex("passed")) ? "PASS" : "FAIL")
                    .append(" |\n");
        }
        md.append("\n");
    }

    private static void appendRecommendations(StringBuilder md, List<Row> rows) {
        md.append("## Recommendations\n\n");

        List<Row> benchmark = filterSpark31Benchmark(rows);
        List<Row> validation = filterSource(rows, "validation");

        List<String> recommendations = new ArrayList<>();

        long failedValidation = validation.stream()
                .filter(row -> !row.getBoolean(row.fieldIndex("passed")))
                .count();
        if (failedValidation > 0) {
            recommendations.add("Исправить ошибки валидации (" + failedValidation
                    + " проверок не пройдено) перед интерпретацией performance-метрик.");
        }

        Map<String, Map<String, Double>> p50 = benchmark.stream()
                .collect(Collectors.groupingBy(
                        row -> row.getString(row.fieldIndex("scenario")),
                        Collectors.toMap(
                                row -> nullableString(row, "format"),
                                row -> row.getDouble(row.fieldIndex("p50_duration_ms")),
                                (left, right) -> left
                        )
                ));

        for (Map.Entry<String, Map<String, Double>> entry : p50.entrySet()) {
            Double orc = entry.getValue().get("orc");
            Double carbon = entry.getValue().get("carbon");
            if (orc != null && carbon != null) {
                if (carbon < orc * 0.9) {
                    recommendations.add("Сценарий `" + entry.getKey()
                            + "`: CarbonData быстрее ORC на p50 — рассмотреть CarbonData для этого паттерна запросов.");
                } else if (orc < carbon * 0.9) {
                    recommendations.add("Сценарий `" + entry.getKey()
                            + "`: ORC быстрее CarbonData на p50 — рассмотреть ORC для этого паттерна запросов.");
                }
            }
        }

        if (recommendations.isEmpty()) {
            recommendations.add("Существенных различий между ORC и CarbonData по p50 на Spark 3.1.1 не обнаружено; "
                    + "выбор формата может определяться индексами и операционными требованиями.");
        }

        for (String recommendation : recommendations) {
            md.append("- ").append(recommendation).append("\n");
        }
        md.append("\n");
    }

    private static String nullableString(Row row, String field) {
        try {
            int idx = row.fieldIndex(field);
            return row.isNullAt(idx) ? "-" : row.getString(idx);
        } catch (IllegalArgumentException ex) {
            return "-";
        }
    }

    private static String formatDouble(Row row, String field) {
        int idx = row.fieldIndex(field);
        if (row.isNullAt(idx)) {
            return "-";
        }
        return String.format("%.2f", row.getDouble(idx));
    }
}
