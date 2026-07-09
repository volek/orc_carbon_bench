package ru.sber.orcbench.generator;

import ru.sber.orcbench.config.AppConfig;
import ru.sber.orcbench.config.CarbonWriteSettings;
import ru.sber.orcbench.config.OrcWriteSettings;
import ru.sber.orcbench.config.OutputFormat;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Set;

public record GeneratorConfig(
        String orcPath,
        String carbonPath,
        Set<OutputFormat> outputFormats,
        long targetSizeTb,
        long seed,
        long avgRowBytes,
        int chunkDays,
        long timestampStartEpochMs,
        long timestampEndEpochMs,
        int targetFileSizeMb,
        OrcWriteSettings orcWrite,
        CarbonWriteSettings carbonWrite
) {
    private static final long BYTES_PER_TB = 1L << 40;

    public static GeneratorConfig from(AppConfig appConfig) {
        return new GeneratorConfig(
                appConfig.orcPath(),
                appConfig.carbonPath(),
                appConfig.outputFormats(),
                appConfig.targetSizeTb(),
                appConfig.seed(),
                appConfig.avgRowBytes(),
                appConfig.chunkDays(),
                appConfig.timestampStartEpochMs(),
                appConfig.timestampEndEpochMs(),
                appConfig.targetFileSizeMb(),
                appConfig.orcWrite(),
                appConfig.carbonWrite()
        );
    }

    public boolean writesOrc() {
        return outputFormats.contains(OutputFormat.ORC);
    }

    public boolean writesCarbon() {
        return outputFormats.contains(OutputFormat.CARBON);
    }

    public long targetBytes() {
        return targetSizeTb * BYTES_PER_TB;
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
