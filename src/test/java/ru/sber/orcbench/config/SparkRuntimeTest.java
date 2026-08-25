package ru.sber.orcbench.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SparkRuntimeTest {

    @Test
    void runtimeIdIsSpark32Orc() {
        assertEquals(SparkRuntime.SPARK32_ORC, SparkRuntime.runtimeId());
    }
}
