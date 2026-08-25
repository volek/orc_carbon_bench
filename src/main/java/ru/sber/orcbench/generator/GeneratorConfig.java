package ru.sber.orcbench.generator;

import ru.sber.orcbench.config.AppConfig;
import ru.sber.orcbench.config.OrcWriteSettings;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

public final class GeneratorConfig {
    private static final long BYTES_PER_TB = 1L << 40;

    private final String orcPath;
    private final double targetSizeTb;
    private final long seed;
    private final long avgRowBytes;
    private final int chunkDays;
    private final long timestampStartEpochMs;
    private final long timestampEndEpochMs;
    private final int targetFileSizeMb;
    private final OrcWriteSettings orcWrite;

    public GeneratorConfig(
            String orcPath,
            double targetSizeTb,
            long seed,
            long avgRowBytes,
            int chunkDays,
            long timestampStartEpochMs,
            long timestampEndEpochMs,
            int targetFileSizeMb,
            OrcWriteSettings orcWrite
    ) {
        this.orcPath = orcPath;
        this.targetSizeTb = targetSizeTb;
        this.seed = seed;
        this.avgRowBytes = avgRowBytes;
        this.chunkDays = chunkDays;
        this.timestampStartEpochMs = timestampStartEpochMs;
        this.timestampEndEpochMs = timestampEndEpochMs;
        this.targetFileSizeMb = targetFileSizeMb;
        this.orcWrite = orcWrite;
    }

    public static GeneratorConfig from(AppConfig appConfig) {
        return new GeneratorConfig(
                appConfig.orcPath(),
                appConfig.targetSizeTb(),
                appConfig.seed(),
                appConfig.avgRowBytes(),
                appConfig.chunkDays(),
                appConfig.timestampStartEpochMs(),
                appConfig.timestampEndEpochMs(),
                appConfig.targetFileSizeMb(),
                appConfig.orcWrite()
        );
    }

    public String orcPath() {
        return orcPath;
    }

    public double targetSizeTb() {
        return targetSizeTb;
    }

    public long seed() {
        return seed;
    }

    public long avgRowBytes() {
        return avgRowBytes;
    }

    public int chunkDays() {
        return chunkDays;
    }

    public long timestampStartEpochMs() {
        return timestampStartEpochMs;
    }

    public long timestampEndEpochMs() {
        return timestampEndEpochMs;
    }

    public int targetFileSizeMb() {
        return targetFileSizeMb;
    }

    public OrcWriteSettings orcWrite() {
        return orcWrite;
    }

    public long targetBytes() {
        return Math.max(1L, Math.round(targetSizeTb * (double) BYTES_PER_TB));
    }

    public long estimatedTotalRows() {
        return Math.max(1L, targetBytes() / avgRowBytes);
    }

    public long timeRangeMs() {
        return Math.max(1L, timestampEndEpochMs - timestampStartEpochMs);
    }

    public int chunkCount() {
        long chunkMs = chunkDays * 86_400_000L;
        return (int) Math.max(1L, (timeRangeMs() + chunkMs - 1) / chunkMs);
    }

    public long rowsPerChunk() {
        return Math.max(1L, (estimatedTotalRows() + chunkCount() - 1) / chunkCount());
    }

    public static long parseEpochMs(String raw, String key) {
        try {
            if (raw.chars().allMatch(Character::isDigit)) {
                return Long.parseLong(raw);
            }
            return LocalDate.parse(raw).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        } catch (Exception ex) {
            throw new IllegalArgumentException("Invalid date/epoch for --" + key + ": " + raw, ex);
        }
    }

    public static long defaultTimestampStart() {
        return LocalDate.of(2024, 1, 1).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    }

    public static long defaultTimestampEnd() {
        return LocalDate.of(2025, 1, 1).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    }

    public Instant timestampStart() {
        return Instant.ofEpochMilli(timestampStartEpochMs);
    }

    public Instant timestampEnd() {
        return Instant.ofEpochMilli(timestampEndEpochMs);
    }
}
