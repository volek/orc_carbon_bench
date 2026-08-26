package ru.sber.orcbench.report;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.OptionalDouble;
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
        md.append("Сводный отчёт бенчмарка ORC на кластерном Spark 3.2.\n\n");

        appendSection(md, "Benchmark Summary", filterSource(rows, "benchmark"));
        appendValidationSection(md, filterSource(rows, "validation"));
        appendRecommendations(md, rows);

        return md.toString();
    }

    private static List<Row> filterSource(List<Row> rows, String source) {
        return rows.stream()
                .filter(row -> source.equals(row.getString(row.fieldIndex("source"))))
                .sorted(Comparator.comparing((Row r) -> r.getString(r.fieldIndex("scenario")))
                        .thenComparing(r -> nullableString(r, "spark_runtime")))
                .collect(Collectors.toList());
    }

    private static void appendSection(StringBuilder md, String title, List<Row> rows) {
        md.append("## ").append(title).append("\n\n");
        if (rows.isEmpty()) {
            md.append("_Нет данных_\n\n");
            return;
        }

        md.append("| scenario | spark_runtime | runs | avg_ms | p50_ms | p95_ms | avg_selectivity |\n");
        md.append("|---|---|---:|---:|---:|---:|---:|\n");
        for (Row row : rows) {
            md.append("| ").append(row.getString(row.fieldIndex("scenario")))
                    .append(" | ").append(nullableString(row, "spark_runtime"))
                    .append(" | ").append(row.getLong(row.fieldIndex("runs")))
                    .append(" | ").append(formatDouble(row, "avg_duration_ms"))
                    .append(" | ").append(formatDouble(row, "p50_duration_ms"))
                    .append(" | ").append(formatDouble(row, "p95_duration_ms"))
                    .append(" | ").append(formatDouble(row, "avg_selectivity"))
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

        List<Row> validation = filterSource(rows, "validation");
        List<String> recommendations = new ArrayList<>();

        long failedValidation = validation.stream()
                .filter(row -> !row.getBoolean(row.fieldIndex("passed")))
                .count();
        if (failedValidation > 0) {
            recommendations.add("Исправить ошибки валидации (" + failedValidation
                    + " проверок не пройдено) перед интерпретацией performance-метрик.");
        }

        List<Row> benchmark = filterSource(rows, "benchmark");
        if (!benchmark.isEmpty()) {
            Row slowest = benchmark.stream()
                    .max(Comparator.comparingDouble(row ->
                            asDouble(row, "p50_duration_ms").orElse(0.0)))
                    .orElse(null);
            if (slowest != null) {
                asDouble(slowest, "p50_duration_ms").ifPresent(p50 ->
                        recommendations.add("Самый медленный сценарий по p50: `"
                                + slowest.getString(slowest.fieldIndex("scenario"))
                                + "` (" + String.format(Locale.US, "%.2f", p50)
                                + " ms) — кандидат на оптимизацию фильтров/проекций ORC.")
                );
            }
        }

        if (recommendations.isEmpty()) {
            recommendations.add("Метрики ORC на Spark 3.2 собраны; сравнивайте сценарии по p50/p95 "
                    + "и selectivity для выбора паттернов запросов.");
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
        OptionalDouble value = asDouble(row, field);
        return value.isPresent() ? String.format(Locale.US, "%.2f", value.getAsDouble()) : "-";
    }

    /** Accepts Double/Long/Integer (percentile_approx often returns Long). */
    private static OptionalDouble asDouble(Row row, String field) {
        int idx = row.fieldIndex(field);
        if (row.isNullAt(idx)) {
            return OptionalDouble.empty();
        }
        Object value = row.get(idx);
        if (value instanceof Number) {
            return OptionalDouble.of(((Number) value).doubleValue());
        }
        return OptionalDouble.empty();
    }
}
