package ru.sber.orcbench.benchmark;

import ru.sber.orcbench.config.IndexProfile;

import java.time.Instant;

public final class IndexBuildMetric {
    private final String runId;
    private final IndexProfile profile;
    private final String indexName;
    private final String indexType;
    private final String columnName;
    private final long buildTimeMs;
    private final Instant executedAt;

    public IndexBuildMetric(
            String runId,
            IndexProfile profile,
            String indexName,
            String indexType,
            String columnName,
            long buildTimeMs,
            Instant executedAt
    ) {
        this.runId = runId;
        this.profile = profile;
        this.indexName = indexName;
        this.indexType = indexType;
        this.columnName = columnName;
        this.buildTimeMs = buildTimeMs;
        this.executedAt = executedAt;
    }

    public String runId() {
        return runId;
    }

    public IndexProfile profile() {
        return profile;
    }

    public String indexName() {
        return indexName;
    }

    public String indexType() {
        return indexType;
    }

    public String columnName() {
        return columnName;
    }

    public long buildTimeMs() {
        return buildTimeMs;
    }

    public Instant executedAt() {
        return executedAt;
    }
}
