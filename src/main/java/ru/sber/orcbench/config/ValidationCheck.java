package ru.sber.orcbench.config;

import java.util.EnumSet;
import java.util.Set;

public enum ValidationCheck {
    ROW_COUNT("row_count"),
    LOW_CARDINALITY_BOUNDS("low_cardinality_bounds"),
    TIMESTAMP_RANGE("timestamp_range"),
    LOG_FORMAT_DISTRIBUTION("log_format_distribution"),
    LOG_MESSAGE_STRUCTURE("log_message_structure");

    private final String cliValue;

    ValidationCheck(String cliValue) {
        this.cliValue = cliValue;
    }

    public String cliValue() {
        return cliValue;
    }

    public static ValidationCheck fromCli(String value) {
        for (ValidationCheck check : values()) {
            if (check.cliValue.equalsIgnoreCase(value.trim())) {
                return check;
            }
        }
        throw new IllegalArgumentException("Unknown validation check: " + value);
    }

    public static Set<ValidationCheck> parseCsv(String raw) {
        if ("all".equalsIgnoreCase(raw.trim())) {
            return EnumSet.allOf(ValidationCheck.class);
        }
        EnumSet<ValidationCheck> checks = EnumSet.noneOf(ValidationCheck.class);
        for (String part : ArgParser.parseCsv(raw)) {
            checks.add(fromCli(part));
        }
        if (checks.isEmpty()) {
            throw new IllegalArgumentException("Invalid argument for --validation-checks: empty list");
        }
        return checks;
    }
}
