package ru.sber.orcbench.config;

import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReportSettingsTest {

    @Test
    void usesDefaultsWhenArgumentsMissing() {
        ReportSettings settings = ReportSettings.from(Map.of());

        assertEquals(EnumSet.allOf(ReportFormat.class), settings.formats());
        assertEquals("benchmark-report", settings.reportName());
    }

    @Test
    void parsesCustomFormatsAndName() {
        ReportSettings settings = ReportSettings.from(Map.of(
                "report-formats", "csv,markdown",
                "report-name", "custom-report"
        ));

        assertEquals(EnumSet.of(ReportFormat.CSV, ReportFormat.MARKDOWN), settings.formats());
        assertEquals("custom-report", settings.reportName());
    }

    @Test
    void parsesAllReportFormats() {
        ReportSettings settings = ReportSettings.from(Map.of(
                "report-formats", "parquet,csv,json,markdown"
        ));

        assertTrue(settings.formats().containsAll(EnumSet.allOf(ReportFormat.class)));
    }
}
