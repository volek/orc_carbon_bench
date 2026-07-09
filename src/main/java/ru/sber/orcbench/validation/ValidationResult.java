package ru.sber.orcbench.validation;

import ru.sber.orcbench.config.ValidationCheck;

import java.time.Instant;

public record ValidationResult(
        String runId,
        ValidationCheck check,
        boolean passed,
        String message,
        String details,
        Instant executedAt
) {
    public static ValidationResult pass(String runId, ValidationCheck check, String message, String details) {
        return new ValidationResult(runId, check, true, message, details, Instant.now());
    }

    public static ValidationResult fail(String runId, ValidationCheck check, String message, String details) {
        return new ValidationResult(runId, check, false, message, details, Instant.now());
    }
}
