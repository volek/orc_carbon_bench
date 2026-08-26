package ru.sber.orcbench.benchmark;

import java.time.Instant;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Derives a selective {@code [start, end)} window inside the data generation range.
 * Using the full generate span as the filter made {@code filter_timestamp_range} match all rows.
 */
public final class TimestampWindow {
    private TimestampWindow() {
    }

    /**
     * @param dataStartMs inclusive data generation start
     * @param dataEndMs   exclusive data generation end
     * @param seed        seed for reproducible window placement
     * @param windowDays  desired window length in days (clamped to the data span)
     * @return pair {@code [start, end)} as Instants
     */
    public static Instant[] selective(long dataStartMs, long dataEndMs, long seed, int windowDays) {
        if (dataEndMs <= dataStartMs) {
            throw new IllegalArgumentException("dataEndMs must be greater than dataStartMs");
        }
        if (windowDays <= 0) {
            throw new IllegalArgumentException("windowDays must be positive: " + windowDays);
        }

        long spanMs = dataEndMs - dataStartMs;
        long requestedMs = TimeUnit.DAYS.toMillis(windowDays);
        long windowMs = Math.min(requestedMs, spanMs);
        long maxOffset = spanMs - windowMs;
        Random random = new Random(seed);
        long offset = maxOffset <= 0 ? 0L : Math.floorMod(random.nextLong(), maxOffset + 1L);
        long startMs = dataStartMs + offset;
        return new Instant[]{
                Instant.ofEpochMilli(startMs),
                Instant.ofEpochMilli(startMs + windowMs)
        };
    }
}
