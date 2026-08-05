package ru.sber.orcbench.config;

import java.util.EnumSet;
import java.util.Set;

public final class ValidationSettings {
    private final Set<ValidationCheck> checks;
    private final double sampleFraction;
    private final double logFormatShareTolerance;

    public ValidationSettings(
            Set<ValidationCheck> checks,
            double sampleFraction,
            double logFormatShareTolerance
    ) {
        this.checks = checks;
        this.sampleFraction = sampleFraction;
        this.logFormatShareTolerance = logFormatShareTolerance;
    }

    public static ValidationSettings defaults() {
        return new ValidationSettings(
                EnumSet.allOf(ValidationCheck.class),
                0.01,
                0.15
        );
    }

    public static ValidationSettings from(java.util.Map<String, String> kv) {
        Set<ValidationCheck> checks = kv.containsKey("validation-checks")
                ? ValidationCheck.parseCsv(kv.get("validation-checks"))
                : defaults().checks();

        double sampleFraction = parseSampleFraction(kv.getOrDefault("validation-sample-fraction", "0.01"));
        double tolerance = parseShareTolerance(kv.getOrDefault("log-format-share-tolerance", "0.15"));

        return new ValidationSettings(checks, sampleFraction, tolerance);
    }

    public Set<ValidationCheck> checks() {
        return checks;
    }

    public double sampleFraction() {
        return sampleFraction;
    }

    public double logFormatShareTolerance() {
        return logFormatShareTolerance;
    }

    private static double parseSampleFraction(String raw) {
        try {
            double value = Double.parseDouble(raw.trim());
            if (value <= 0.0 || value > 1.0) {
                throw new IllegalArgumentException("Argument --validation-sample-fraction must be in (0, 1]: " + raw);
            }
            return value;
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid numeric argument for --validation-sample-fraction: " + raw, ex);
        }
    }

    private static double parseShareTolerance(String raw) {
        try {
            double value = Double.parseDouble(raw.trim());
            if (value < 0.0 || value > 1.0) {
                throw new IllegalArgumentException("Argument --log-format-share-tolerance must be in [0, 1]: " + raw);
            }
            return value;
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid numeric argument for --log-format-share-tolerance: " + raw, ex);
        }
    }
}
