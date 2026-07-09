package ru.sber.orcbench.config;

import java.util.EnumSet;
import java.util.Set;

public record ReportSettings(
        Set<ReportFormat> formats,
        String reportName
) {
    public static ReportSettings from(java.util.Map<String, String> kv) {
        Set<ReportFormat> formats = kv.containsKey("report-formats")
                ? ReportFormat.parseCsv(kv.get("report-formats"))
                : EnumSet.allOf(ReportFormat.class);

        String reportName = kv.getOrDefault("report-name", "benchmark-report");
        return new ReportSettings(formats, reportName);
    }
}
