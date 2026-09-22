package ru.sber.orcbench.config;

import java.util.Locale;
import java.util.Map;

/**
 * Factor-experiment labels written into every benchmark row.
 */
public final class ExperimentMeta {
    public static final long DEFAULT_SLA_THRESHOLD_MS = 3000L;
    /** Soft ceiling for ST/archive Abyss-style scans (not AUDEI interactive API). */
    public static final long DEFAULT_ARCHIVE_SLA_THRESHOLD_MS = 120_000L;
    public static final long MAX_RESPONSE_ROWS = 2000L;

    private final String layoutId;
    private final EngineType engine;
    private final CacheState cacheState;
    private final long slaThresholdMs;
    private final long archiveSlaThresholdMs;
    private final long datasetBytes;

    public ExperimentMeta(
            String layoutId,
            EngineType engine,
            CacheState cacheState,
            long slaThresholdMs,
            long datasetBytes
    ) {
        this(layoutId, engine, cacheState, slaThresholdMs, DEFAULT_ARCHIVE_SLA_THRESHOLD_MS, datasetBytes);
    }

    public ExperimentMeta(
            String layoutId,
            EngineType engine,
            CacheState cacheState,
            long slaThresholdMs,
            long archiveSlaThresholdMs,
            long datasetBytes
    ) {
        this.layoutId = layoutId;
        this.engine = engine;
        this.cacheState = cacheState;
        this.slaThresholdMs = slaThresholdMs;
        this.archiveSlaThresholdMs = archiveSlaThresholdMs;
        this.datasetBytes = datasetBytes;
    }

    public static ExperimentMeta defaults() {
        return new ExperimentMeta(
                "default",
                EngineType.SPARK,
                CacheState.COLD,
                DEFAULT_SLA_THRESHOLD_MS,
                DEFAULT_ARCHIVE_SLA_THRESHOLD_MS,
                0L
        );
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
        long archiveSlaMs = kv.containsKey("archive-sla-threshold-ms")
                ? ArgParser.parsePositiveLong(kv.get("archive-sla-threshold-ms"), "archive-sla-threshold-ms")
                : DEFAULT_ARCHIVE_SLA_THRESHOLD_MS;
        return new ExperimentMeta(
                layoutId,
                EngineType.fromCli(kv.get("engine")),
                CacheState.fromCli(kv.getOrDefault("cache-state", "cold")),
                slaMs,
                archiveSlaMs,
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

    /**
     * ST duration_group label from search window length in days.
     */
    public static String durationGroup(int windowDays) {
        if (windowDays <= 0) {
            return "0_unknown";
        }
        if (windowDays <= 31) {
            return "1_month";
        }
        if (windowDays <= 92) {
            return "2_quarter";
        }
        if (windowDays <= 183) {
            return "3_halfyear";
        }
        return "4_more";
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

    public long archiveSlaThresholdMs() {
        return archiveSlaThresholdMs;
    }

    public long datasetBytes() {
        return datasetBytes;
    }

    public double scanRatio(long bytesRead) {
        if (datasetBytes <= 0L || bytesRead < 0L) {
            return 0.0d;
        }
        return (double) bytesRead / (double) datasetBytes;
    }

    public long slaThresholdFor(String slaClass) {
        if ("archive".equals(slaClass)) {
            return archiveSlaThresholdMs;
        }
        return slaThresholdMs;
    }

    public boolean slaOk(long durationMs) {
        return durationMs <= slaThresholdMs;
    }

    public boolean slaOk(long durationMs, String slaClass) {
        return durationMs <= slaThresholdFor(slaClass);
    }

    /**
     * ST metric: seconds per GB of bytes actually read (ε-safe).
     */
    public static double secondsPerGb(long durationMs, long bytesRead) {
        double seconds = durationMs / 1000.0d;
        double gb = bytesRead / (1024.0d * 1024.0d * 1024.0d);
        if (gb < 1e-12d) {
            return Double.NaN;
        }
        return seconds / gb;
    }
}
