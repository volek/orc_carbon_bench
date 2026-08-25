package ru.sber.orcbench.config;

import java.util.EnumSet;
import java.util.Set;

public enum BenchmarkScenario {
    FULL_SCAN("full_scan"),
    PROJECTION("projection"),
    FILTER_LOW_CARDINALITY("filter_low_cardinality"),
    FILTER_MEDIUM_CARDINALITY("filter_medium_cardinality"),
    FILTER_HIGH_CARDINALITY("filter_high_cardinality"),
    FILTER_TIMESTAMP_RANGE("filter_timestamp_range"),
    FILTER_LOG_FORMAT("filter_log_format"),
    FILTER_COMBINED("filter_combined"),
    GROUP_BY("group_by"),
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
        EnumSet<BenchmarkScenario> scenarios = EnumSet.noneOf(BenchmarkScenario.class);
        for (String part : ArgParser.parseCsv(raw)) {
            scenarios.add(fromCli(part));
        }
        if (scenarios.isEmpty()) {
            throw new IllegalArgumentException("Invalid argument for --benchmark-scenarios: empty list");
        }
        return scenarios;
    }
}
