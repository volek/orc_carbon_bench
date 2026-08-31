package ru.sber.orcbench.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrcWriteSettingsTest {

    @Test
    void parseBloomColumnsDefaultList() {
        String[] columns = OrcWriteSettings.parseBloomFilterColumns("event_id,user_id,product_id,campaign_id");
        assertArrayEquals(OrcWriteSettings.DEFAULT_BLOOM_FILTER_COLUMNS, columns);
    }

    @Test
    void parseBloomColumnsNoneDisables() {
        assertEquals(0, OrcWriteSettings.parseBloomFilterColumns("none").length);
    }

    @Test
    void bloomEnabledWhenColumnsPresent() {
        OrcWriteSettings settings = new OrcWriteSettings("snappy", 64, 32, 0,
                OrcWriteSettings.DEFAULT_PARTITION_BY,
                OrcWriteSettings.DEFAULT_BLOOM_FILTER_COLUMNS,
                0.05d);
        assertTrue(settings.bloomFiltersEnabled());
        assertEquals("event_id,user_id,product_id,campaign_id", settings.bloomFilterColumnsCsv());
    }

    @Test
    void bloomDisabledWhenEmpty() {
        OrcWriteSettings settings = new OrcWriteSettings("snappy", 64, 32, 0,
                OrcWriteSettings.DEFAULT_PARTITION_BY,
                new String[0],
                0.05d);
        assertFalse(settings.bloomFiltersEnabled());
    }
}
