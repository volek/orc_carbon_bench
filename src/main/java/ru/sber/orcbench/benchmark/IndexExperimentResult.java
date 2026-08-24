package ru.sber.orcbench.benchmark;

import ru.sber.orcbench.config.BenchmarkScenario;
import ru.sber.orcbench.config.IndexProfile;
import ru.sber.orcbench.config.OutputFormat;
import ru.sber.orcbench.config.SparkRuntimeInfo;

import java.time.Instant;

public final class IndexExperimentResult {
    private final String runId;
    private final BenchmarkScenario scenario;
    private final OutputFormat format;
    private final IndexProfile indexProfile;
    private final String logFormat;
    private final int runIndex;
    private final long durationMs;
    private final long rowsReturned;
    private final long totalRows;
    private final double selectivity;
    private final long seed;
    private final Instant executedAt;
    private final String sparkVersion;
    private final String sparkRuntime;

    public IndexExperimentResult(
            String runId,
            BenchmarkScenario scenario,
            OutputFormat format,
            IndexProfile indexProfile,
            String logFormat,
            int runIndex,
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
        this.indexProfile = indexProfile;
        this.logFormat = logFormat;
        this.runIndex = runIndex;
        this.durationMs = durationMs;
        this.rowsReturned = rowsReturned;
        this.totalRows = totalRows;
        this.selectivity = selectivity;
        this.seed = seed;
        this.executedAt = executedAt;
        this.sparkVersion = sparkVersion;
        this.sparkRuntime = sparkRuntime;
    }

    public static IndexExperimentResult of(
            String runId,
            BenchmarkScenario scenario,
            OutputFormat format,
            IndexProfile indexProfile,
            String logFormat,
            int runIndex,
            long durationMs,
            long rowsReturned,
            long totalRows,
            long seed,
            SparkRuntimeInfo runtime
    ) {
        double selectivity = totalRows == 0 ? 0.0 : (double) rowsReturned / totalRows;
        return new IndexExperimentResult(
                runId,
                scenario,
                format,
                indexProfile,
                logFormat,
                runIndex,
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

    public IndexProfile indexProfile() {
        return indexProfile;
    }

    public String logFormat() {
        return logFormat;
    }

    public int runIndex() {
        return runIndex;
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
