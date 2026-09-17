package ru.sber.orcbench.config;

import java.util.EnumSet;
import java.util.Set;

/**
 * Benchmark query suite mapped to document Q1–Q10 (existing column names).
 *
 * <pre>
 * Q1  partition_prune
 * Q2  filter_high_cardinality
 * Q3  filter_medium_cardinality
 * Q4  filter_low_cardinality
 * Q5  filter_in
 * Q6  filter_timestamp_range
 * Q7  projection vs full_scan
 * Q8  group_by
 * Q9  group_by_heavy
 * Q10 join_dictionary
 * </pre>
 */
public enum BenchmarkScenario {
    FULL_SCAN("full_scan"),
    PROJECTION("projection"),
    PARTITION_PRUNE("partition_prune"),
    FILTER_LOW_CARDINALITY("filter_low_cardinality"),
    FILTER_MEDIUM_CARDINALITY("filter_medium_cardinality"),
    FILTER_HIGH_CARDINALITY("filter_high_cardinality"),
    FILTER_IN("filter_in"),
    FILTER_TIMESTAMP_RANGE("filter_timestamp_range"),
    FILTER_LOG_FORMAT("filter_log_format"),
    FILTER_COMBINED("filter_combined"),
    GROUP_BY("group_by"),
    GROUP_BY_HEAVY("group_by_heavy"),
    JOIN_DICTIONARY("join_dictionary"),
    TEXT_SEARCH("text_search");

    private final String cliValue;

    BenchmarkScenario(String cliValue) {
        this.cliValue = cliValue;
    }

    public String cliValue() {
        return cliValue;
    }

    public static BenchmarkScenario fromCli(String value) {
        String normalized = value.trim().toLowerCase(java.util.Locale.ROOT);
        for (BenchmarkScenario scenario : values()) {
            if (scenario.cliValue.equals(normalized)) {
                return scenario;
            }
        }
        throw new IllegalArgumentException("Unknown benchmark scenario: " + value);
    }

    public static Set<BenchmarkScenario> parseCsv(String raw) {
        if ("all".equalsIgnoreCase(raw.trim())) {
            return EnumSet.allOf(BenchmarkScenario.class);
        }
        if ("doc".equalsIgnoreCase(raw.trim()) || "q1-q10".equalsIgnoreCase(raw.trim())) {
            return documentSuite();
        }
        EnumSet<BenchmarkScenario> scenarios = EnumSet.noneOf(BenchmarkScenario.class);
        for (String part : ArgParser.parseCsv(raw)) {
            scenarios.add(fromCli(part));
        }
        if (scenarios.isEmpty()) {
            throw new IllegalArgumentException("Invalid argument for --benchmark-scenarios: empty list");
        }
        return scenarios;
    }

    /** Document Q1–Q10 core suite (excluding auxiliary log_format/text_search). */
    public static Set<BenchmarkScenario> documentSuite() {
        return EnumSet.of(
                PARTITION_PRUNE,
                FILTER_HIGH_CARDINALITY,
                FILTER_MEDIUM_CARDINALITY,
                FILTER_LOW_CARDINALITY,
                FILTER_IN,
                FILTER_TIMESTAMP_RANGE,
                PROJECTION,
                FULL_SCAN,
                GROUP_BY,
                GROUP_BY_HEAVY,
                JOIN_DICTIONARY
        );
    }
}
