package ru.sber.orcbench.validation;

import ru.sber.orcbench.config.SparkRuntimeInfo;
import ru.sber.orcbench.config.ValidationCheck;

import java.time.Instant;

public final class ValidationResult {
    private final String runId;
    private final ValidationCheck check;
    private final boolean passed;
    private final String message;
    private final String details;
    private final Instant executedAt;
    private final String sparkVersion;
    private final String sparkRuntime;

    public ValidationResult(
            String runId,
            ValidationCheck check,
            boolean passed,
            String message,
            String details,
            Instant executedAt,
            String sparkVersion,
            String sparkRuntime
    ) {
        this.runId = runId;
        this.check = check;
        this.passed = passed;
        this.message = message;
        this.details = details;
        this.executedAt = executedAt;
        this.sparkVersion = sparkVersion;
        this.sparkRuntime = sparkRuntime;
    }

    public static ValidationResult pass(
            String runId,
            ValidationCheck check,
            String message,
            String details,
            SparkRuntimeInfo runtime
    ) {
        return new ValidationResult(
                runId, check, true, message, details, Instant.now(),
                runtime.sparkVersion(), runtime.sparkRuntime()
        );
    }

    public static ValidationResult fail(
            String runId,
            ValidationCheck check,
            String message,
            String details,
            SparkRuntimeInfo runtime
    ) {
        return new ValidationResult(
                runId, check, false, message, details, Instant.now(),
                runtime.sparkVersion(), runtime.sparkRuntime()
        );
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

    public String sparkVersion() {
        return sparkVersion;
    }

    public String sparkRuntime() {
        return sparkRuntime;
    }
}
