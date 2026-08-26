package ru.sber.orcbench.config;

import java.util.EnumSet;
import java.util.Set;

public final class BenchmarkSettings {
    public static final int DEFAULT_TIMESTAMP_WINDOW_DAYS = 30;

    private final int warmupRuns;
    private final int repeatRuns;
    private final Set<BenchmarkScenario> scenarios;
    private final boolean clearCacheBetweenRuns;
    private final int timestampWindowDays;

    public BenchmarkSettings(
            int warmupRuns,
            int repeatRuns,
            Set<BenchmarkScenario> scenarios,
            boolean clearCacheBetweenRuns,
            int timestampWindowDays
    ) {
        this.warmupRuns = warmupRuns;
        this.repeatRuns = repeatRuns;
        this.scenarios = scenarios;
        this.clearCacheBetweenRuns = clearCacheBetweenRuns;
        this.timestampWindowDays = timestampWindowDays;
    }

    public static BenchmarkSettings defaults() {
        return new BenchmarkSettings(
                1,
                3,
                EnumSet.allOf(BenchmarkScenario.class),
                true,
                DEFAULT_TIMESTAMP_WINDOW_DAYS
        );
    }

    public static Set<BenchmarkScenario> parseScenarios(String raw) {
        return BenchmarkScenario.parseCsv(raw);
    }

    public int warmupRuns() {
        return warmupRuns;
    }

    public int repeatRuns() {
        return repeatRuns;
    }

    public Set<BenchmarkScenario> scenarios() {
        return scenarios;
    }

    public boolean clearCacheBetweenRuns() {
        return clearCacheBetweenRuns;
    }

    public int timestampWindowDays() {
        return timestampWindowDays;
    }
}
