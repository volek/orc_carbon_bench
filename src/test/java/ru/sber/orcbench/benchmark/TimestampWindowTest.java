package ru.sber.orcbench.benchmark;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TimestampWindowTest {

    private static final long START = Instant.parse("2024-01-01T00:00:00Z").toEpochMilli();
    private static final long END = Instant.parse("2025-01-01T00:00:00Z").toEpochMilli();

    @Test
    void selectiveWindowIsInsideGenerateSpanAndHasRequestedLength() {
        Instant[] window = TimestampWindow.selective(START, END, 42L, 30);
        long lengthMs = window[1].toEpochMilli() - window[0].toEpochMilli();

        assertEquals(TimeUnit.DAYS.toMillis(30), lengthMs);
        assertTrue(window[0].toEpochMilli() >= START);
        assertTrue(window[1].toEpochMilli() <= END);
        assertTrue(window[0].toEpochMilli() < window[1].toEpochMilli());
    }

    @Test
    void sameSeedIsReproducible() {
        Instant[] a = TimestampWindow.selective(START, END, 42L, 30);
        Instant[] b = TimestampWindow.selective(START, END, 42L, 30);
        assertEquals(a[0], b[0]);
        assertEquals(a[1], b[1]);
    }

    @Test
    void differentSeedsCanMoveWindow() {
        Instant[] a = TimestampWindow.selective(START, END, 1L, 30);
        Instant[] b = TimestampWindow.selective(START, END, 2L, 30);
        // Extremely unlikely equal; if equal, still valid but assert length only as soft check
        assertEquals(
                a[1].toEpochMilli() - a[0].toEpochMilli(),
                b[1].toEpochMilli() - b[0].toEpochMilli()
        );
        assertTrue(!a[0].equals(b[0]) || !a[1].equals(b[1]));
    }

    @Test
    void windowClampedToDataSpan() {
        Instant[] window = TimestampWindow.selective(START, END, 7L, 400);
        assertEquals(START, window[0].toEpochMilli());
        assertEquals(END, window[1].toEpochMilli());
    }

    @Test
    void rejectsInvalidBounds() {
        assertThrows(IllegalArgumentException.class, () -> TimestampWindow.selective(END, START, 1L, 7));
        assertThrows(IllegalArgumentException.class, () -> TimestampWindow.selective(START, END, 1L, 0));
    }
}
