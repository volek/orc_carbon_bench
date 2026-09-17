package ru.sber.orcbench.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparkExecSettingsTest {

    @Test
    void defaultsAreOptimizedOn() {
        SparkExecSettings settings = SparkExecSettings.defaults();
        assertTrue(settings.filterPushdown());
        assertTrue(settings.vectorizedReader());
        assertTrue(settings.adaptiveExecution());
        assertTrue(settings.dynamicPartitionPruning());
        assertTrue(settings.costBasedOptimization());
    }

    @Test
    void parsesTogglesFromMap() {
        java.util.Map<String, String> kv = new java.util.HashMap<>();
        kv.put("spark-orc-filter-pushdown", "false");
        kv.put("spark-orc-vectorized", "false");
        kv.put("spark-aqe", "false");
        kv.put("spark-dpp", "true");
        kv.put("spark-cbo", "true");
        SparkExecSettings settings = SparkExecSettings.from(kv);
        assertFalse(settings.filterPushdown());
        assertFalse(settings.vectorizedReader());
        assertFalse(settings.adaptiveExecution());
        assertTrue(settings.dynamicPartitionPruning());
        assertTrue(settings.costBasedOptimization());
    }
}
