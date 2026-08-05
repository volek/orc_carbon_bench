package ru.sber.orcbench.config;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReportSettingsTest {

    @Test
    void usesDefaultsWhenArgumentsMissing() {
        ReportSettings settings = ReportSettings.from(Collections.<String, String>emptyMap());

        assertEquals(EnumSet.allOf(ReportFormat.class), settings.formats());
        assertEquals("benchmark-report", settings.reportName());
    }

    @Test
    void parsesCustomFormatsAndName() {
        Map<String, String> args = new HashMap<>();
        args.put("report-formats", "csv,markdown");
        args.put("report-name", "custom-report");
        ReportSettings settings = ReportSettings.from(args);

        assertEquals(EnumSet.of(ReportFormat.CSV, ReportFormat.MARKDOWN), settings.formats());
        assertEquals("custom-report", settings.reportName());
    }

    @Test
    void parsesAllReportFormats() {
        Map<String, String> args = new HashMap<>();
        args.put("report-formats", "parquet,csv,json,markdown");
        ReportSettings settings = ReportSettings.from(args);

        assertTrue(settings.formats().containsAll(EnumSet.allOf(ReportFormat.class)));
    }
}
