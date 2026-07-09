package ru.sber.orcbench.config;

import java.util.EnumSet;
import java.util.Set;

public record BenchmarkSettings(
        int warmupRuns,
        int repeatRuns,
        Set<BenchmarkScenario> scenarios,
        boolean clearCacheBetweenRuns,
        Set<OutputFormat> formats
) {
    public static BenchmarkSettings defaults() {
        return new BenchmarkSettings(
                1,
                3,
                EnumSet.allOf(BenchmarkScenario.class),
                true,
                EnumSet.allOf(OutputFormat.class)
        );
    }

    public static Set<BenchmarkScenario> parseScenarios(String raw) {
        if ("all".equalsIgnoreCase(raw.trim())) {
            return EnumSet.allOf(BenchmarkScenario.class);
        }
        EnumSet<BenchmarkScenario> scenarios = EnumSet.noneOf(BenchmarkScenario.class);
        for (String part : ArgParser.parseCsv(raw)) {
            scenarios.add(BenchmarkScenario.fromCli(part));
        }
        if (scenarios.isEmpty()) {
            throw new IllegalArgumentException("Invalid argument for --benchmark-scenarios: empty list");
        }
        return scenarios;
    }

    public static Set<OutputFormat> parseFormats(String raw) {
        return OutputFormat.parseCsv(raw);
    }
}
