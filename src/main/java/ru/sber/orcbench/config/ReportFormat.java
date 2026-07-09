package ru.sber.orcbench.config;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum ReportFormat {
    PARQUET("parquet"),
    CSV("csv"),
    JSON("json"),
    MARKDOWN("markdown");

    private static final Map<String, ReportFormat> BY_CLI = Arrays.stream(values())
            .collect(Collectors.toMap(f -> f.cliValue, Function.identity()));

    private final String cliValue;

    ReportFormat(String cliValue) {
        this.cliValue = cliValue;
    }

    public String cliValue() {
        return cliValue;
    }

    public static ReportFormat fromCli(String value) {
        ReportFormat format = BY_CLI.get(value.trim().toLowerCase(Locale.ROOT));
        if (format == null) {
            throw new IllegalArgumentException("Unknown report format: " + value);
        }
        return format;
    }

    public static Set<ReportFormat> parseCsv(String raw) {
        EnumSet<ReportFormat> formats = EnumSet.noneOf(ReportFormat.class);
        for (String part : ArgParser.parseCsv(raw)) {
            formats.add(fromCli(part));
        }
        if (formats.isEmpty()) {
            throw new IllegalArgumentException("Invalid argument for --report-formats: empty list");
        }
        return formats;
    }
}
