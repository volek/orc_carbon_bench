package ru.sber.orcbench.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExperimentMetaTest {

    @Test
    void normalizesLayoutId() {
        assertEquals("b0", ExperimentMeta.normalizeLayoutId("B0"));
        assertEquals("best_orc", ExperimentMeta.normalizeLayoutId("best_orc"));
    }

    @Test
    void rejectsInvalidLayoutId() {
        assertThrows(IllegalArgumentException.class, () -> ExperimentMeta.normalizeLayoutId(""));
        assertThrows(IllegalArgumentException.class, () -> ExperimentMeta.normalizeLayoutId("Bad Id"));
    }

    @Test
    void computesScanRatioAndSla() {
        ExperimentMeta meta = new ExperimentMeta("b0", EngineType.SPARK, CacheState.COLD, 3000L, 1_000_000L);
        assertEquals(0.1d, meta.scanRatio(100_000L), 1e-9);
        assertTrue(meta.slaOk(2500L));
        assertFalse(meta.slaOk(3001L));
    }

    @Test
    void parsesFromArgsMap() {
        java.util.Map<String, String> kv = new java.util.HashMap<>();
        kv.put("mode", "benchmark");
        kv.put("layout-id", "F1");
        kv.put("engine", "hive_llap");
        kv.put("cache-state", "warm");
        kv.put("sla-threshold-ms", "3000");
        kv.put("dataset-bytes", "2048");
        ExperimentMeta meta = ExperimentMeta.from(kv);
        assertEquals("f1", meta.layoutId());
        assertEquals(EngineType.HIVE_LLAP, meta.engine());
        assertEquals(CacheState.WARM, meta.cacheState());
        assertEquals(2048L, meta.datasetBytes());
    }
}
