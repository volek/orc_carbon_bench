package ru.sber.orcbench.config;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BenchmarkScenarioTest {

    @Test
    void parsesDocumentSuiteAlias() {
        Set<BenchmarkScenario> scenarios = BenchmarkScenario.parseCsv("doc");
        assertTrue(scenarios.contains(BenchmarkScenario.PARTITION_PRUNE));
        assertTrue(scenarios.contains(BenchmarkScenario.FILTER_IN));
        assertTrue(scenarios.contains(BenchmarkScenario.JOIN_DICTIONARY));
        assertTrue(scenarios.contains(BenchmarkScenario.GROUP_BY_HEAVY));
        assertEquals(11, scenarios.size());
    }

    @Test
    void parsesNewScenarioNames() {
        assertEquals(BenchmarkScenario.FILTER_IN, BenchmarkScenario.fromCli("filter_in"));
        assertEquals(BenchmarkScenario.JOIN_DICTIONARY, BenchmarkScenario.fromCli("join_dictionary"));
    }
}
