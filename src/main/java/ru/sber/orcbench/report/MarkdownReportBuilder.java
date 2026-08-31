package ru.sber.orcbench.report;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.stream.Collectors;

public final class MarkdownReportBuilder {
    private static final List<String> BLOOM_SCENARIOS = Arrays.asList(
            "filter_high_cardinality",
            "filter_medium_cardinality",
            "filter_combined"
    );

    private MarkdownReportBuilder() {
    }

    public static String build(Dataset<Row> summary, String reportName) {
        return build(summary.collectAsList(), reportName);
    }

    public static String build(List<Row> rows, String reportName) {
        StringBuilder md = new StringBuilder();
        md.append("# ").append(reportName).append("\n\n");
        md.append("Сводный отчёт бенчмарка ORC на кластерном Spark 3.2.\n\n");

        List<Row> benchmark = filterSource(rows, "benchmark");
        appendSection(md, "Benchmark Summary", benchmark);
        appendBloomSummary(md, benchmark);
        appendValidationSection(md, filterSource(rows, "validation"));
        appendRecommendations(md, rows);

        return md.toString();
    }

    private static List<Row> filterSource(List<Row> rows, String source) {
        return rows.stream()
                .filter(row -> source.equals(row.getString(row.fieldIndex("source"))))
                .sorted(Comparator.comparing((Row r) -> r.getString(r.fieldIndex("scenario")))
                        .thenComparing(r -> nullableString(r, "dataset_label"))
                        .thenComparing(r -> nullableString(r, "spark_runtime")))
                .collect(Collectors.toList());
    }

    private static void appendSection(StringBuilder md, String title, List<Row> rows) {
        md.append("## ").append(title).append("\n\n");
        if (rows.isEmpty()) {
            md.append("_Нет данных_\n\n");
            return;
        }

        boolean hasDatasetLabel = rows.stream().anyMatch(row -> hasField(row, "dataset_label"));
        boolean hasIo = rows.stream().anyMatch(row -> asDouble(row, "avg_bytes_read").isPresent());

        md.append("| scenario | dataset | bloom_columns | spark_runtime | runs | avg_ms | p50_ms | p95_ms | avg_selectivity");
        if (hasIo) {
            md.append(" | avg_bytes_read | avg_records_read");
        }
        md.append(" |\n");
        md.append("|---|---|---|---|---:|---:|---:|---:|---:");
        if (hasIo) {
            md.append("|---:|---:");
        }
        md.append("|\n");

        for (Row row : rows) {
            md.append("| ").append(row.getString(row.fieldIndex("scenario")))
                    .append(" | ").append(hasDatasetLabel ? nullableString(row, "dataset_label") : "-")
                    .append(" | ").append(formatBloomColumns(row))
                    .append(" | ").append(nullableString(row, "spark_runtime"))
                    .append(" | ").append(row.getLong(row.fieldIndex("runs")))
                    .append(" | ").append(formatDouble(row, "avg_duration_ms"))
                    .append(" | ").append(formatDouble(row, "p50_duration_ms"))
                    .append(" | ").append(formatDouble(row, "p95_duration_ms"))
                    .append(" | ").append(formatDouble(row, "avg_selectivity"));
            if (hasIo) {
                md.append(" | ").append(formatDouble(row, "avg_bytes_read"))
                        .append(" | ").append(formatDouble(row, "avg_records_read"));
            }
            md.append(" |\n");
        }
        md.append("\n");
    }

    private static void appendBloomSummary(StringBuilder md, List<Row> benchmark) {
        md.append("## Bloom filter comparison\n\n");

        Set<String> labels = benchmark.stream()
                .map(row -> nullableString(row, "dataset_label"))
                .collect(Collectors.toCollection(HashSet::new));

        boolean hasBloomAb = labels.contains("bloom") && labels.contains("nobloom");
        if (!hasBloomAb) {
            md.append("_Нет A/B данных (нужны прогоны с `dataset_label=bloom` и `dataset_label=nobloom`)._\n\n");
            return;
        }

        md.append("| scenario | nobloom bytes_read | bloom bytes_read | Δ bytes | nobloom p50_ms | bloom p50_ms |\n");
        md.append("|---|---:|---:|---:|---:|---:|\n");

        for (String scenario : BLOOM_SCENARIOS) {
            Row nobloom = findBenchmarkRow(benchmark, scenario, "nobloom");
            Row bloom = findBenchmarkRow(benchmark, scenario, "bloom");
            if (nobloom == null && bloom == null) {
                continue;
            }
            md.append("| ").append(scenario)
                    .append(" | ").append(formatDouble(nobloom, "avg_bytes_read"))
                    .append(" | ").append(formatDouble(bloom, "avg_bytes_read"))
                    .append(" | ").append(formatBytesDelta(nobloom, bloom))
                    .append(" | ").append(formatDouble(nobloom, "p50_duration_ms"))
                    .append(" | ").append(formatDouble(bloom, "p50_duration_ms"))
                    .append(" |\n");
        }
        md.append("\n");
    }

    private static Row findBenchmarkRow(List<Row> benchmark, String scenario, String datasetLabel) {
        return benchmark.stream()
                .filter(row -> scenario.equals(row.getString(row.fieldIndex("scenario")))
                        && datasetLabel.equals(nullableString(row, "dataset_label")))
                .findFirst()
                .orElse(null);
    }

    private static String formatBytesDelta(Row nobloom, Row bloom) {
        OptionalDouble noBytes = nobloom == null ? OptionalDouble.empty() : asDouble(nobloom, "avg_bytes_read");
        OptionalDouble bloomBytes = bloom == null ? OptionalDouble.empty() : asDouble(bloom, "avg_bytes_read");
        if (!noBytes.isPresent() || !bloomBytes.isPresent() || noBytes.getAsDouble() <= 0) {
            return "-";
        }
        double pct = (bloomBytes.getAsDouble() - noBytes.getAsDouble()) / noBytes.getAsDouble() * 100.0;
        return String.format(Locale.US, "%+.1f%%", pct);
    }

    private static String formatBloomColumns(Row row) {
        if (!hasField(row, "orc_bloom_columns")) {
            return "-";
        }
        String value = nullableString(row, "orc_bloom_columns");
        return "none".equals(value) || "-".equals(value) ? "none" : value;
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
        appendBloomRecommendations(recommendations, benchmark);

        if (!benchmark.isEmpty()) {
            Row slowest = benchmark.stream()
                    .max(Comparator.comparingDouble(row ->
                            asDouble(row, "p50_duration_ms").orElse(0.0)))
                    .orElse(null);
            if (slowest != null) {
                asDouble(slowest, "p50_duration_ms").ifPresent(p50 ->
                        recommendations.add("Самый медленный сценарий по p50: `"
                                + slowest.getString(slowest.fieldIndex("scenario"))
                                + "` / dataset="
                                + nullableString(slowest, "dataset_label")
                                + " (" + String.format(Locale.US, "%.2f", p50)
                                + " ms).")
                );
            }
        }

        if (recommendations.isEmpty()) {
            recommendations.add("Метрики ORC на Spark 3.2 собраны; сравнивайте bloom vs nobloom по avg_bytes_read "
                    + "на `filter_high_cardinality` и `filter_medium_cardinality`.");
        }

        for (String recommendation : recommendations) {
            md.append("- ").append(recommendation).append("\n");
        }
        md.append("\n");
    }

    private static void appendBloomRecommendations(List<String> recommendations, List<Row> benchmark) {
        Row nobloomHigh = findBenchmarkRow(benchmark, "filter_high_cardinality", "nobloom");
        Row bloomHigh = findBenchmarkRow(benchmark, "filter_high_cardinality", "bloom");
        if (nobloomHigh == null || bloomHigh == null) {
            return;
        }

        OptionalDouble noBytes = asDouble(nobloomHigh, "avg_bytes_read");
        OptionalDouble bloomBytes = asDouble(bloomHigh, "avg_bytes_read");
        if (noBytes.isPresent() && bloomBytes.isPresent() && noBytes.getAsDouble() > 0) {
            double reduction = (1.0 - bloomBytes.getAsDouble() / noBytes.getAsDouble()) * 100.0;
            recommendations.add(String.format(Locale.US,
                    "Bloom A/B на `filter_high_cardinality`: avg_bytes_read bloom=%.0f vs nobloom=%.0f (экономия ~%.1f%%).",
                    bloomBytes.getAsDouble(), noBytes.getAsDouble(), reduction));
        }
    }

    private static boolean hasField(Row row, String field) {
        try {
            row.fieldIndex(field);
            return true;
        } catch (IllegalArgumentException ex) {
            return false;
        }
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
        if (row == null) {
            return "-";
        }
        OptionalDouble value = asDouble(row, field);
        return value.isPresent() ? String.format(Locale.US, "%.2f", value.getAsDouble()) : "-";
    }

    /** Accepts Double/Long/Integer (percentile_approx often returns Long). */
    private static OptionalDouble asDouble(Row row, String field) {
        try {
            int idx = row.fieldIndex(field);
            if (row.isNullAt(idx)) {
                return OptionalDouble.empty();
            }
            Object value = row.get(idx);
            if (value instanceof Number) {
                return OptionalDouble.of(((Number) value).doubleValue());
            }
            return OptionalDouble.empty();
        } catch (IllegalArgumentException ex) {
            return OptionalDouble.empty();
        }
    }
}
