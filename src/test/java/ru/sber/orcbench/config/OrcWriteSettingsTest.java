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
    void parsePartitionByNoneDisables() {
        assertEquals(0, OrcWriteSettings.parsePartitionBy("none").length);
    }

    @Test
    void parseSortColumnsAndStride() {
        assertArrayEquals(new String[]{"event_id", "timestamp"}, OrcWriteSettings.parseSortColumns("event_id,timestamp"));
        assertEquals(0, OrcWriteSettings.parseSortColumns("none").length);
        assertEquals(5000, OrcWriteSettings.parseRowIndexStride("5000"));
        assertEquals(OrcWriteSettings.DEFAULT_ROW_INDEX_STRIDE, OrcWriteSettings.parseRowIndexStride(null));
    }

    @Test
    void bloomEnabledWhenColumnsPresent() {
        OrcWriteSettings settings = new OrcWriteSettings("snappy", 64, 32, 0,
                OrcWriteSettings.DEFAULT_PARTITION_BY,
                OrcWriteSettings.DEFAULT_BLOOM_FILTER_COLUMNS,
                0.05d);
        assertTrue(settings.bloomFiltersEnabled());
        assertEquals("event_id,user_id,product_id,campaign_id", settings.bloomFilterColumnsCsv());
        assertFalse(settings.sorted());
        assertEquals(OrcWriteSettings.DEFAULT_ROW_INDEX_STRIDE, settings.rowIndexStride());
    }

    @Test
    void bloomDisabledWhenEmpty() {
        OrcWriteSettings settings = new OrcWriteSettings("snappy", 64, 32, 0,
                OrcWriteSettings.DEFAULT_PARTITION_BY,
                new String[0],
                0.05d);
        assertFalse(settings.bloomFiltersEnabled());
    }

    @Test
    void fullConstructorKeepsSortAndStride() {
        OrcWriteSettings settings = new OrcWriteSettings(
                "zstd", 128, 32, 5000, 8,
                new String[]{"event_year", "event_month", "event_day"},
                OrcWriteSettings.BLOOM_HIGH_COLUMNS,
                0.01d,
                new String[]{"event_id"}
        );
        assertTrue(settings.sorted());
        assertEquals("event_id", settings.sortColumnsCsv());
        assertEquals(5000, settings.rowIndexStride());
        assertEquals("zstd", settings.compression());
    }
}
