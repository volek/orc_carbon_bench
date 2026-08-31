package ru.sber.orcbench.generator;

import org.junit.jupiter.api.Test;
import ru.sber.orcbench.config.AppConfig;
import ru.sber.orcbench.config.OrcWriteSettings;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GeneratorConfigTest {

    @Test
    void estimatesRowsFromTargetSize() {
        AppConfig config = minimalConfig(5.0d, 512);
        GeneratorConfig generatorConfig = GeneratorConfig.from(config);

        long expected = 5L * (1L << 40) / 512L;
        assertEquals(expected, generatorConfig.estimatedTotalRows());
    }

    @Test
    void estimatesRowsFromFractionalTargetSize() {
        AppConfig config = minimalConfig(0.01d, 512);
        GeneratorConfig generatorConfig = GeneratorConfig.from(config);

        long expected = Math.round(0.01d * (1L << 40)) / 512L;
        assertEquals(expected, generatorConfig.estimatedTotalRows());
    }

    @Test
    void chunkCountUsesConfiguredDays() {
        AppConfig config = minimalConfig(1.0d, 512);
        GeneratorConfig generatorConfig = GeneratorConfig.from(config);

        assertTrue(generatorConfig.chunkCount() >= 365);
    }

    private static AppConfig minimalConfig(double targetTb, long avgRowBytes) {
        return new AppConfig(
                ru.sber.orcbench.config.Mode.GENERATE,
                ru.sber.orcbench.config.StoragePaths.from("/bench", null, null, null, null),
                targetTb,
                42L,
                avgRowBytes,
                1,
                GeneratorConfig.defaultTimestampStart(),
                GeneratorConfig.defaultTimestampEnd(),
                384,
                new OrcWriteSettings(
                        "snappy",
                        64,
                        32,
                        0,
                        OrcWriteSettings.DEFAULT_PARTITION_BY,
                        OrcWriteSettings.DEFAULT_BLOOM_FILTER_COLUMNS,
                        0.05d
                ),
                ru.sber.orcbench.config.BenchmarkSettings.defaults(),
                ru.sber.orcbench.config.ValidationSettings.defaults(),
                ru.sber.orcbench.config.ReportSettings.from(Collections.<String, String>emptyMap()),
                "default"
        );
    }
}
