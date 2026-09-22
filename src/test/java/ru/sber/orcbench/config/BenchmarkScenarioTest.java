package ru.sber.orcbench.config;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BenchmarkScenarioTest {

    @Test
    void parsesDocumentSuiteAlias() {
        Set<BenchmarkScenario> scenarios = BenchmarkScenario.parseCsv("doc");
        assertTrue(scenarios.contains(BenchmarkScenario.PARTITION_PRUNE));
        assertTrue(scenarios.contains(BenchmarkScenario.FILTER_IN));
        assertTrue(scenarios.contains(BenchmarkScenario.JOIN_DICTIONARY));
        assertTrue(scenarios.contains(BenchmarkScenario.GROUP_BY_HEAVY));
        assertFalse(scenarios.contains(BenchmarkScenario.EPK_EQ_14D));
        assertEquals(11, scenarios.size());
    }

    @Test
    void parsesAudeiSuite() {
        Set<BenchmarkScenario> scenarios = BenchmarkScenario.parseCsv("audei");
        assertEquals(5, scenarios.size());
        assertTrue(scenarios.contains(BenchmarkScenario.EPK_EQ_1D));
        assertTrue(scenarios.contains(BenchmarkScenario.EPK_EQ_14D));
        assertTrue(scenarios.contains(BenchmarkScenario.EPK_PAGE));
        assertTrue(scenarios.contains(BenchmarkScenario.EQ_FILTERS));
        assertTrue(scenarios.contains(BenchmarkScenario.ORDER_BY_EPK_DAY));
        assertEquals("interactive", BenchmarkScenario.EPK_EQ_14D.slaClass());
        assertEquals("7_EQ", BenchmarkScenario.EPK_EQ_14D.queryCategory());
        assertEquals("1_month", BenchmarkScenario.EPK_EQ_14D.durationGroup());
    }

    @Test
    void parsesStSuite() {
        Set<BenchmarkScenario> scenarios = BenchmarkScenario.parseCsv("st");
        assertEquals(7, scenarios.size());
        assertTrue(scenarios.contains(BenchmarkScenario.NO_FILTER));
        assertTrue(scenarios.contains(BenchmarkScenario.LIKE_FULLTEXT));
        assertTrue(scenarios.contains(BenchmarkScenario.RLIKE));
        assertEquals("8_NO_FILTER", BenchmarkScenario.NO_FILTER.queryCategory());
        assertEquals("3_LIKE_FULLTEXT", BenchmarkScenario.LIKE_FULLTEXT.queryCategory());
        assertEquals("archive", BenchmarkScenario.LIKE_FULLTEXT.slaClass());
    }

    @Test
    void parsesNewScenarioNames() {
        assertEquals(BenchmarkScenario.FILTER_IN, BenchmarkScenario.fromCli("filter_in"));
        assertEquals(BenchmarkScenario.JOIN_DICTIONARY, BenchmarkScenario.fromCli("join_dictionary"));
        assertEquals(BenchmarkScenario.EPK_EQ_14D, BenchmarkScenario.fromCli("epk_eq_14d"));
        assertEquals(BenchmarkScenario.LIKE_FULLTEXT, BenchmarkScenario.fromCli("like_fulltext"));
    }
}
