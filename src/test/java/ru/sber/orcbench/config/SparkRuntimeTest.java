package ru.sber.orcbench.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.condition.EnabledIf;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparkRuntimeTest {

    static boolean carbonPresent() {
        return SparkRuntime.carbonAvailable();
    }

    @Test
    void runtimeIdMatchesCarbonAvailability() {
        if (SparkRuntime.carbonAvailable()) {
            assertEquals(SparkRuntime.SPARK31_CARBON, SparkRuntime.runtimeId());
        } else {
            assertEquals(SparkRuntime.SPARK32_ORC, SparkRuntime.runtimeId());
        }
    }

    @Test
    @EnabledIf("carbonPresent")
    void allowsCarbonModesWhenCarbonIsOnClasspath() {
        AppConfig config = AppConfig.fromArgs(new String[]{
                "--mode=validate",
                "--base-path=/tmp/bench"
        });
        assertTrue(SparkRuntime.requiresCarbon(config));
        assertDoesNotThrow(() -> SparkRuntime.requireCompatible(config));
    }

    @Test
    @DisabledIf("carbonPresent")
    void rejectsCarbonModesWhenCarbonIsMissing() {
        AppConfig validate = AppConfig.fromArgs(new String[]{
                "--mode=validate",
                "--base-path=/tmp/bench"
        });
        AppConfig index = AppConfig.fromArgs(new String[]{
                "--mode=index-experiment",
                "--base-path=/tmp/bench"
        });
        AppConfig generateCarbon = AppConfig.fromArgs(new String[]{
                "--mode=generate",
                "--output-formats=carbon",
                "--base-path=/tmp/bench"
        });
        AppConfig benchmarkCarbon = AppConfig.fromArgs(new String[]{
                "--mode=benchmark",
                "--formats=orc,carbon",
                "--base-path=/tmp/bench"
        });

        assertThrows(IllegalStateException.class, () -> SparkRuntime.requireCompatible(validate));
        assertThrows(IllegalStateException.class, () -> SparkRuntime.requireCompatible(index));
        assertThrows(IllegalStateException.class, () -> SparkRuntime.requireCompatible(generateCarbon));
        assertThrows(IllegalStateException.class, () -> SparkRuntime.requireCompatible(benchmarkCarbon));
    }

    @Test
    @DisabledIf("carbonPresent")
    void allowsOrcOnlyModesWhenCarbonIsMissing() {
        AppConfig generateOrc = AppConfig.fromArgs(new String[]{
                "--mode=generate",
                "--output-formats=orc",
                "--base-path=/tmp/bench"
        });
        AppConfig benchmarkOrc = AppConfig.fromArgs(new String[]{
                "--mode=benchmark",
                "--formats=orc",
                "--base-path=/tmp/bench"
        });
        AppConfig report = AppConfig.fromArgs(new String[]{
                "--mode=report",
                "--base-path=/tmp/bench"
        });

        assertFalse(SparkRuntime.requiresCarbon(generateOrc));
        assertFalse(SparkRuntime.requiresCarbon(benchmarkOrc));
        assertFalse(SparkRuntime.requiresCarbon(report));
        assertDoesNotThrow(() -> SparkRuntime.requireCompatible(generateOrc));
        assertDoesNotThrow(() -> SparkRuntime.requireCompatible(benchmarkOrc));
        assertDoesNotThrow(() -> SparkRuntime.requireCompatible(report));
        assertEquals("/tmp/bench/reports/raw/spark32-orc", SparkRuntime.benchmarkOutputPath(benchmarkOrc));
    }
}
