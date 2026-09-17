package ru.sber.orcbench.benchmark;

import ru.sber.orcbench.config.BenchmarkScenario;
import ru.sber.orcbench.config.ExperimentMeta;
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
    private final String datasetLabel;
    private final String orcPath;
    private final String orcBloomColumns;
    private final Instant executedAt;
    private final String sparkVersion;
    private final String sparkRuntime;
    private final String layoutId;
    private final String engine;
    private final String cacheState;
    private final double scanRatio;
    private final boolean slaOk;
    private final long slaThresholdMs;

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
            String datasetLabel,
            String orcPath,
            String orcBloomColumns,
            Instant executedAt,
            String sparkVersion,
            String sparkRuntime,
            String layoutId,
            String engine,
            String cacheState,
            double scanRatio,
            boolean slaOk,
            long slaThresholdMs
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
        this.datasetLabel = datasetLabel;
        this.orcPath = orcPath;
        this.orcBloomColumns = orcBloomColumns;
        this.executedAt = executedAt;
        this.sparkVersion = sparkVersion;
        this.sparkRuntime = sparkRuntime;
        this.layoutId = layoutId;
        this.engine = engine;
        this.cacheState = cacheState;
        this.scanRatio = scanRatio;
        this.slaOk = slaOk;
        this.slaThresholdMs = slaThresholdMs;
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
            String datasetLabel,
            String orcPath,
            String orcBloomColumns,
            SparkRuntimeInfo runtime,
            ExperimentMeta experiment
    ) {
        double selectivity = totalRows == 0 ? 0.0 : (double) rowsReturned / totalRows;
        ExperimentMeta meta = experiment == null ? ExperimentMeta.defaults() : experiment;
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
                datasetLabel,
                orcPath,
                orcBloomColumns,
                Instant.now(),
                runtime.sparkVersion(),
                runtime.sparkRuntime(),
                meta.layoutId(),
                meta.engine().cliValue(),
                meta.cacheState().cliValue(),
                meta.scanRatio(bytesRead),
                meta.slaOk(durationMs),
                meta.slaThresholdMs()
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

    public String datasetLabel() {
        return datasetLabel;
    }

    public String orcPath() {
        return orcPath;
    }

    public String orcBloomColumns() {
        return orcBloomColumns;
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

    public String layoutId() {
        return layoutId;
    }

    public String engine() {
        return engine;
    }

    public String cacheState() {
        return cacheState;
    }

    public double scanRatio() {
        return scanRatio;
    }

    public boolean slaOk() {
        return slaOk;
    }

    public long slaThresholdMs() {
        return slaThresholdMs;
    }
}
