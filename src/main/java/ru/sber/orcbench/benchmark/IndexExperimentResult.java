package ru.sber.orcbench.benchmark;

import ru.sber.orcbench.config.BenchmarkScenario;
import ru.sber.orcbench.config.IndexProfile;
import ru.sber.orcbench.config.OutputFormat;

import java.time.Instant;

public record IndexExperimentResult(
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
        Instant executedAt
) {
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
            long seed
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
                Instant.now()
        );
    }
}
