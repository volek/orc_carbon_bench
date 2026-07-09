package ru.sber.orcbench.config;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

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
    LUCENE_TEXT_SEARCH("lucene_text_search");

    private static final Map<String, BenchmarkScenario> BY_CLI = Arrays.stream(values())
            .collect(Collectors.toMap(s -> s.cliValue, Function.identity()));

    private final String cliValue;

    BenchmarkScenario(String cliValue) {
        this.cliValue = cliValue;
    }

    public String cliValue() {
        return cliValue;
    }

    public static BenchmarkScenario fromCli(String value) {
        BenchmarkScenario scenario = BY_CLI.get(value.trim().toLowerCase(Locale.ROOT));
        if (scenario == null) {
            throw new IllegalArgumentException("Unknown benchmark scenario: " + value);
        }
        return scenario;
    }
}
