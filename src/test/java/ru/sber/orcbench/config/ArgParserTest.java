package ru.sber.orcbench.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArgParserTest {

    @Test
    void parseBooleanAcceptsCommonValues() {
        assertTrue(ArgParser.parseBoolean("true", "flag"));
        assertTrue(ArgParser.parseBoolean("1", "flag"));
        assertFalse(ArgParser.parseBoolean("false", "flag"));
        assertFalse(ArgParser.parseBoolean("no", "flag"));
    }

    @Test
    void parseBooleanRejectsUnknownValue() {
        assertThrows(IllegalArgumentException.class, () -> ArgParser.parseBoolean("maybe", "flag"));
    }

    @Test
    void parseCsvSplitsAndTrims() {
        assertEquals(3, ArgParser.parseCsv("a, b ,c").length);
        assertEquals("b", ArgParser.parseCsv("a, b ,c")[1]);
    }

    @Test
    void parsePositiveLongRejectsNonPositive() {
        assertThrows(IllegalArgumentException.class, () -> ArgParser.parsePositiveLong("0", "size"));
    }

    @Test
    void parsePositiveDoubleAcceptsFractionalTb() {
        assertEquals(0.01d, ArgParser.parsePositiveDouble("0.01", "target-size-tb"), 1e-12);
    }

    @Test
    void parsePositiveDoubleRejectsNonPositive() {
        assertThrows(IllegalArgumentException.class, () -> ArgParser.parsePositiveDouble("0", "target-size-tb"));
        assertThrows(IllegalArgumentException.class, () -> ArgParser.parsePositiveDouble("-1", "target-size-tb"));
    }
}
