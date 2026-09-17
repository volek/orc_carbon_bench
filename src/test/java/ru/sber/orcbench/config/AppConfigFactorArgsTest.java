package ru.sber.orcbench.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AppConfigFactorArgsTest {

    @Test
    void parsesLayoutExperimentAndSparkExec() {
        AppConfig config = AppConfig.fromArgs(new String[]{
                "--mode=benchmark",
                "--base-path=hdfs:///bench",
                "--layout-id=F1",
                "--engine=spark",
                "--cache-state=warm",
                "--sla-threshold-ms=3000",
                "--target-size-tb=0.01",
                "--orc-bloom-filter-columns=event_id,user_id",
                "--orc-sort-columns=event_id",
                "--orc-row-index-stride=5000",
                "--partition-by=event_year,event_month,event_day",
                "--spark-orc-filter-pushdown=false",
                "--spark-aqe=true",
                "--benchmark-scenarios=doc"
        });

        assertEquals("f1", config.experiment().layoutId());
        assertEquals(CacheState.WARM, config.experiment().cacheState());
        assertEquals("hdfs:///bench/layouts/f1/orc", config.orcPath());
        assertEquals("hdfs:///bench/layouts/f1/dictionary", config.dictionaryPath());
        assertTrue(config.orcWrite().sorted());
        assertEquals(5000, config.orcWrite().rowIndexStride());
        assertFalse(config.sparkExec().filterPushdown());
        assertTrue(config.sparkExec().adaptiveExecution());
        assertTrue(config.benchmark().scenarios().contains(BenchmarkScenario.JOIN_DICTIONARY));
        assertEquals("f1", config.benchmarkDatasetLabel());
    }

    @Test
    void partitionByNoneProducesUnpartitionedWriteSettings() {
        AppConfig config = AppConfig.fromArgs(new String[]{
                "--mode=generate",
                "--partition-by=none",
                "--orc-bloom-filter-columns=none",
                "--target-size-tb=0.01"
        });
        assertFalse(config.orcWrite().partitioned());
        assertFalse(config.orcWrite().bloomFiltersEnabled());
    }
}
