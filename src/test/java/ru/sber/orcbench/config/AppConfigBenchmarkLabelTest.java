package ru.sber.orcbench.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AppConfigBenchmarkLabelTest {

    @Test
    void infersBloomLabelFromOrcBloomPath() {
        assertEquals("bloom", AppConfig.inferBenchmarkDatasetLabel("hdfs:///base/orc_bloom"));
    }

    @Test
    void infersNobloomLabelFromOrcPath() {
        assertEquals("nobloom", AppConfig.inferBenchmarkDatasetLabel("hdfs:///base/orc"));
    }

    @Test
    void infersLabelFromOrcWriteSettings() {
        assertEquals("bloom", AppConfig.inferDatasetLabelFromOrcWrite(new OrcWriteSettings(
                "snappy", 64, 32, 0, OrcWriteSettings.DEFAULT_PARTITION_BY,
                OrcWriteSettings.DEFAULT_BLOOM_FILTER_COLUMNS, 0.05d)));
        assertEquals("nobloom", AppConfig.inferDatasetLabelFromOrcWrite(new OrcWriteSettings(
                "snappy", 64, 32, 0, OrcWriteSettings.DEFAULT_PARTITION_BY, new String[0], 0.05d)));
    }
}
