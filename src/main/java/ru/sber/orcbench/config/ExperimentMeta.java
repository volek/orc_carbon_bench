package ru.sber.orcbench.config;

import java.util.Locale;
import java.util.Map;

/**
 * Factor-experiment labels written into every benchmark row.
 */
public final class ExperimentMeta {
    public static final long DEFAULT_SLA_THRESHOLD_MS = 3000L;

    private final String layoutId;
    private final EngineType engine;
    private final CacheState cacheState;
    private final long slaThresholdMs;
    private final long datasetBytes;

    public ExperimentMeta(
            String layoutId,
            EngineType engine,
            CacheState cacheState,
            long slaThresholdMs,
            long datasetBytes
    ) {
        this.layoutId = layoutId;
        this.engine = engine;
        this.cacheState = cacheState;
        this.slaThresholdMs = slaThresholdMs;
        this.datasetBytes = datasetBytes;
    }

    public static ExperimentMeta defaults() {
        return new ExperimentMeta("default", EngineType.SPARK, CacheState.COLD, DEFAULT_SLA_THRESHOLD_MS, 0L);
    }

    public static ExperimentMeta from(Map<String, String> kv) {
        String layoutId = kv.containsKey("layout-id")
                ? normalizeLayoutId(kv.get("layout-id"))
                : "default";
        long datasetBytes = 0L;
        if (kv.containsKey("dataset-bytes")) {
            datasetBytes = ArgParser.parsePositiveLong(kv.get("dataset-bytes"), "dataset-bytes");
        } else if (kv.containsKey("target-size-tb")) {
            double tb = ArgParser.parsePositiveDouble(kv.get("target-size-tb"), "target-size-tb");
            datasetBytes = Math.max(1L, Math.round(tb * (1L << 40)));
        }
        long slaMs = kv.containsKey("sla-threshold-ms")
                ? ArgParser.parsePositiveLong(kv.get("sla-threshold-ms"), "sla-threshold-ms")
                : DEFAULT_SLA_THRESHOLD_MS;
        return new ExperimentMeta(
                layoutId,
                EngineType.fromCli(kv.get("engine")),
                CacheState.fromCli(kv.getOrDefault("cache-state", "cold")),
                slaMs,
                datasetBytes
        );
    }

    public static String normalizeLayoutId(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            throw new IllegalArgumentException("Invalid --layout-id: empty");
        }
        String normalized = raw.trim().toLowerCase(Locale.ROOT);
        if (!normalized.matches("[a-z0-9][a-z0-9._-]{0,63}")) {
            throw new IllegalArgumentException(
                    "Invalid --layout-id: " + raw + ". Use [a-z0-9][a-z0-9._-]{0,63}"
            );
        }
        return normalized;
    }

    public String layoutId() {
        return layoutId;
    }

    public EngineType engine() {
        return engine;
    }

    public CacheState cacheState() {
        return cacheState;
    }

    public long slaThresholdMs() {
        return slaThresholdMs;
    }

    public long datasetBytes() {
        return datasetBytes;
    }

    /**
     * Scan ratio = physical bytes read / dataset bytes (0 when dataset size unknown).
     */
    public double scanRatio(long bytesRead) {
        if (datasetBytes <= 0L || bytesRead < 0L) {
            return 0.0d;
        }
        return (double) bytesRead / (double) datasetBytes;
    }

    public boolean slaOk(long durationMs) {
        return durationMs <= slaThresholdMs;
    }
}
