package ru.sber.orcbench.benchmark;

import ru.sber.orcbench.config.BenchmarkScenario;
import ru.sber.orcbench.config.OutputFormat;

import java.time.Instant;

public record BenchmarkResult(
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
        Instant executedAt
) {
    public static BenchmarkResult of(
            String runId,
            BenchmarkScenario scenario,
            OutputFormat format,
            int runIndex,
            boolean warmup,
            long durationMs,
            long rowsReturned,
            long totalRows,
            long seed
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
                Instant.now()
        );
    }
}
