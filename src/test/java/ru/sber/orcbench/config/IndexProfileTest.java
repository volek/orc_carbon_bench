package ru.sber.orcbench.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexProfileTest {

    @Test
    void bloomProfileEnablesBloomOnly() {
        CarbonWriteSettings base = new CarbonWriteSettings(
                "bench_events", "snappy", false, CarbonWriteSettings.DEFAULT_BLOOM_COLUMNS,
                false, CarbonWriteSettings.DEFAULT_LUCENE_COLUMNS, 0,
                CarbonWriteSettings.DEFAULT_PARTITION_BY
        );

        CarbonWriteSettings bloom = IndexProfile.BLOOM.applyTo(base);
        assertTrue(bloom.enableBloomIndex());
        assertFalse(bloom.enableLuceneIndex());
    }

    @Test
    void bloomLuceneProfileEnablesBoth() {
        CarbonWriteSettings base = new CarbonWriteSettings(
                "bench_events", "snappy", false, CarbonWriteSettings.DEFAULT_BLOOM_COLUMNS,
                false, CarbonWriteSettings.DEFAULT_LUCENE_COLUMNS, 0,
                CarbonWriteSettings.DEFAULT_PARTITION_BY
        );

        CarbonWriteSettings both = IndexProfile.BLOOM_LUCENE.applyTo(base);
        assertTrue(both.enableBloomIndex());
        assertTrue(both.enableLuceneIndex());
    }
}
