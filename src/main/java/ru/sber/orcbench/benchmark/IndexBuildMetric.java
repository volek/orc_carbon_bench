package ru.sber.orcbench.benchmark;

import ru.sber.orcbench.config.IndexProfile;

import java.time.Instant;

public record IndexBuildMetric(
        String runId,
        IndexProfile profile,
        String indexName,
        String indexType,
        String columnName,
        long buildTimeMs,
        Instant executedAt
) {
}
