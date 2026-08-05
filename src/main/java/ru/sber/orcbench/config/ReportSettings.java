package ru.sber.orcbench.config;

import java.util.EnumSet;
import java.util.Set;

public final class ReportSettings {
    private final Set<ReportFormat> formats;
    private final String reportName;

    public ReportSettings(Set<ReportFormat> formats, String reportName) {
        this.formats = formats;
        this.reportName = reportName;
    }

    public static ReportSettings from(java.util.Map<String, String> kv) {
        Set<ReportFormat> formats = kv.containsKey("report-formats")
                ? ReportFormat.parseCsv(kv.get("report-formats"))
                : EnumSet.allOf(ReportFormat.class);

        String reportName = kv.getOrDefault("report-name", "benchmark-report");
        return new ReportSettings(formats, reportName);
    }

    public Set<ReportFormat> formats() {
        return formats;
    }

    public String reportName() {
        return reportName;
    }
}
