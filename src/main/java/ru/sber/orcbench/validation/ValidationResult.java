package ru.sber.orcbench.validation;

import ru.sber.orcbench.config.ValidationCheck;

import java.time.Instant;

public final class ValidationResult {
    private final String runId;
    private final ValidationCheck check;
    private final boolean passed;
    private final String message;
    private final String details;
    private final Instant executedAt;

    public ValidationResult(
            String runId,
            ValidationCheck check,
            boolean passed,
            String message,
            String details,
            Instant executedAt
    ) {
        this.runId = runId;
        this.check = check;
        this.passed = passed;
        this.message = message;
        this.details = details;
        this.executedAt = executedAt;
    }

    public static ValidationResult pass(String runId, ValidationCheck check, String message, String details) {
        return new ValidationResult(runId, check, true, message, details, Instant.now());
    }

    public static ValidationResult fail(String runId, ValidationCheck check, String message, String details) {
        return new ValidationResult(runId, check, false, message, details, Instant.now());
    }

    public String runId() {
        return runId;
    }

    public ValidationCheck check() {
        return check;
    }

    public boolean passed() {
        return passed;
    }

    public String message() {
        return message;
    }

    public String details() {
        return details;
    }

    public Instant executedAt() {
        return executedAt;
    }
}
