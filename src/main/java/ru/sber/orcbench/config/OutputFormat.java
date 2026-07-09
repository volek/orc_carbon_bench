package ru.sber.orcbench.config;

import java.util.EnumSet;
import java.util.Locale;
import java.util.Set;

public enum OutputFormat {
    ORC("orc"),
    CARBON("carbon");

    private final String cliValue;

    OutputFormat(String cliValue) {
        this.cliValue = cliValue;
    }

    public String cliValue() {
        return cliValue;
    }

    public static Set<OutputFormat> parseCsv(String raw) {
        String[] parts = ArgParser.parseCsv(raw);
        if (parts.length == 0) {
            throw new IllegalArgumentException("Invalid argument for --output-formats: empty list");
        }
        EnumSet<OutputFormat> formats = EnumSet.noneOf(OutputFormat.class);
        for (String part : parts) {
            formats.add(fromCli(part));
        }
        return formats;
    }

    public static OutputFormat fromCli(String value) {
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        for (OutputFormat format : values()) {
            if (format.cliValue.equals(normalized)) {
                return format;
            }
        }
        throw new IllegalArgumentException("Unknown output format: " + value + ". Allowed: orc, carbon");
    }
}
