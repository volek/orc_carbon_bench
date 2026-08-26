package ru.sber.orcbench.benchmark;

import ru.sber.orcbench.config.BenchmarkScenario;
import ru.sber.orcbench.config.SparkRuntimeInfo;

import java.time.Instant;

public final class BenchmarkResult {
    private final String runId;
    private final BenchmarkScenario scenario;
    private final int runIndex;
    private final boolean warmup;
    private final long durationMs;
    private final long rowsReturned;
    private final long totalRows;
    private final double selectivity;
    private final long bytesRead;
    private final long recordsRead;
    private final long seed;
    private final Instant executedAt;
    private final String sparkVersion;
    private final String sparkRuntime;

    public BenchmarkResult(
            String runId,
            BenchmarkScenario scenario,
            int runIndex,
            boolean warmup,
            long durationMs,
            long rowsReturned,
            long totalRows,
            double selectivity,
            long bytesRead,
            long recordsRead,
            long seed,
            Instant executedAt,
            String sparkVersion,
            String sparkRuntime
    ) {
        this.runId = runId;
        this.scenario = scenario;
        this.runIndex = runIndex;
        this.warmup = warmup;
        this.durationMs = durationMs;
        this.rowsReturned = rowsReturned;
        this.totalRows = totalRows;
        this.selectivity = selectivity;
        this.bytesRead = bytesRead;
        this.recordsRead = recordsRead;
        this.seed = seed;
        this.executedAt = executedAt;
        this.sparkVersion = sparkVersion;
        this.sparkRuntime = sparkRuntime;
    }

    public static BenchmarkResult of(
            String runId,
            BenchmarkScenario scenario,
            int runIndex,
            boolean warmup,
            long durationMs,
            long rowsReturned,
            long totalRows,
            long bytesRead,
            long recordsRead,
            long seed,
            SparkRuntimeInfo runtime
    ) {
        double selectivity = totalRows == 0 ? 0.0 : (double) rowsReturned / totalRows;
        return new BenchmarkResult(
                runId,
                scenario,
                runIndex,
                warmup,
                durationMs,
                rowsReturned,
                totalRows,
                selectivity,
                bytesRead,
                recordsRead,
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

    public long bytesRead() {
        return bytesRead;
    }

    public long recordsRead() {
        return recordsRead;
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
