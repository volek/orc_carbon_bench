package ru.sber.orcbench.generator;

import org.junit.jupiter.api.Test;
import ru.sber.orcbench.config.AppConfig;
import ru.sber.orcbench.config.CarbonWriteSettings;
import ru.sber.orcbench.config.OrcWriteSettings;
import ru.sber.orcbench.config.OutputFormat;

import java.util.Collections;
import java.util.EnumSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GeneratorConfigTest {

    @Test
    void estimatesRowsFromTargetSize() {
        AppConfig config = minimalConfig(5, 512);
        GeneratorConfig generatorConfig = GeneratorConfig.from(config);

        long expected = 5L * (1L << 40) / 512L;
        assertEquals(expected, generatorConfig.estimatedTotalRows());
    }

    @Test
    void chunkCountUsesConfiguredDays() {
        AppConfig config = minimalConfig(1, 512);
        GeneratorConfig generatorConfig = GeneratorConfig.from(config);

        assertTrue(generatorConfig.chunkCount() >= 365);
    }

    private static AppConfig minimalConfig(long targetTb, long avgRowBytes) {
        return new AppConfig(
                ru.sber.orcbench.config.Mode.GENERATE,
                ru.sber.orcbench.config.StoragePaths.from("/bench", null, null, null),
                EnumSet.of(OutputFormat.ORC),
                targetTb,
                42L,
                avgRowBytes,
                1,
                GeneratorConfig.defaultTimestampStart(),
                GeneratorConfig.defaultTimestampEnd(),
                384,
                new OrcWriteSettings("snappy", 64, 32, 0, OrcWriteSettings.DEFAULT_PARTITION_BY),
                new CarbonWriteSettings(
                        "bench_events", "snappy", false, CarbonWriteSettings.DEFAULT_BLOOM_COLUMNS,
                        false, CarbonWriteSettings.DEFAULT_LUCENE_COLUMNS, 0,
                        CarbonWriteSettings.DEFAULT_PARTITION_BY
                ),
                ru.sber.orcbench.config.BenchmarkSettings.defaults(),
                new ru.sber.orcbench.config.IndexExperimentSettings(
                        Collections.emptySet(), Collections.emptyMap(), false, 1, 3, true
                ),
                ru.sber.orcbench.config.ValidationSettings.defaults(),
                ru.sber.orcbench.config.ReportSettings.from(Collections.<String, String>emptyMap())
        );
    }
}
