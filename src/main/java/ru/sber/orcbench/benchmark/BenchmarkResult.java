package ru.sber.orcbench.benchmark;

import ru.sber.orcbench.config.BenchmarkScenario;
import ru.sber.orcbench.config.OutputFormat;
import ru.sber.orcbench.config.SparkRuntimeInfo;

import java.time.Instant;

public final class BenchmarkResult {
    private final String runId;
    private final BenchmarkScenario scenario;
    private final OutputFormat format;
    private final int runIndex;
    private final boolean warmup;
    private final long durationMs;
    private final long rowsReturned;
    private final long totalRows;
    private final double selectivity;
    private final long seed;
    private final Instant executedAt;
    private final String sparkVersion;
    private final String sparkRuntime;

    public BenchmarkResult(
            String runId,
            BenchmarkScenario scenario,
            OutputFormat format,
            int runIndex,
            boolean warmup,
            long durationMs,
            long rowsReturned,
            long totalRows,
            double selectivity,
            long seed,
            Instant executedAt,
            String sparkVersion,
            String sparkRuntime
    ) {
        this.runId = runId;
        this.scenario = scenario;
        this.format = format;
        this.runIndex = runIndex;
        this.warmup = warmup;
        this.durationMs = durationMs;
        this.rowsReturned = rowsReturned;
        this.totalRows = totalRows;
        this.selectivity = selectivity;
        this.seed = seed;
        this.executedAt = executedAt;
        this.sparkVersion = sparkVersion;
        this.sparkRuntime = sparkRuntime;
    }

    public static BenchmarkResult of(
            String runId,
            BenchmarkScenario scenario,
            OutputFormat format,
            int runIndex,
            boolean warmup,
            long durationMs,
            long rowsReturned,
            long totalRows,
            long seed,
            SparkRuntimeInfo runtime
    ) {
        double selectivity = totalRows == 0 ? 0.0 : (double) rowsReturned / totalRows;
        return new BenchmarkResult(
                runId,
                scenario,
                format,
                runIndex,
                warmup,
                durationMs,
                rowsReturned,
                totalRows,
                selectivity,
                seed,
                Instant.now(),
                runtime.sparkVersion(),
                runtime.sparkRuntime()
        );
    }

    public String runId() {
        return runId;
    }

    public BenchmarkScenario scenario() {
        return scenario;
    }

    public OutputFormat format() {
        return format;
    }

    public int runIndex() {
        return runIndex;
    }

    public boolean warmup() {
        return warmup;
    }

    public long durationMs() {
        return durationMs;
    }

    public long rowsReturned() {
        return rowsReturned;
    }

    public long totalRows() {
        return totalRows;
    }

    public double selectivity() {
        return selectivity;
    }

    public long seed() {
        return seed;
    }

    public Instant executedAt() {
        return executedAt;
    }

    public String sparkVersion() {
        return sparkVersion;
    }

    public String sparkRuntime() {
        return sparkRuntime;
    }
}
