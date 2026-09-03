package ru.sber.orcbench.validation;

import org.junit.jupiter.api.Test;
import ru.sber.orcbench.config.SparkRuntime;
import ru.sber.orcbench.config.SparkRuntimeInfo;
import ru.sber.orcbench.config.ValidationCheck;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ValidationRunnerTest {

    @Test
    void formatFailureMessageIncludesCheckNameAndDetails() {
        SparkRuntimeInfo runtime = new SparkRuntimeInfo("3.2.1", SparkRuntime.SPARK32_ORC);
        ValidationResult failed = ValidationResult.fail(
                "run-1",
                ValidationCheck.ORC_BLOOM_FILTERS,
                "ORC bloom filter metadata check failed",
                "Bloom filter missing for columns: event_id",
                runtime
        );

        String message = ValidationRunner.formatFailureMessage(6, Collections.singletonList(failed));

        assertTrue(message.startsWith("Validation failed: 1 of 6 checks failed: "));
        assertTrue(message.contains("orc_bloom_filters"));
        assertTrue(message.contains("ORC bloom filter metadata check failed"));
        assertTrue(message.contains("Bloom filter missing for columns: event_id"));
    }

    @Test
    void formatFailureMessageJoinsMultipleFailures() {
        SparkRuntimeInfo runtime = new SparkRuntimeInfo("3.2.1", SparkRuntime.SPARK32_ORC);
        ValidationResult first = ValidationResult.fail(
                "run-1", ValidationCheck.ROW_COUNT, "ORC dataset is empty", "orcRows=0", runtime
        );
        ValidationResult second = ValidationResult.fail(
                "run-1", ValidationCheck.TIMESTAMP_RANGE, "Timestamp range invalid", "minTs=0", runtime
        );

        String message = ValidationRunner.formatFailureMessage(6, Arrays.asList(first, second));

        assertEquals(
                "Validation failed: 2 of 6 checks failed: "
                        + "row_count [ORC dataset is empty; orcRows=0]; "
                        + "timestamp_range [Timestamp range invalid; minTs=0]",
                message
        );
    }
}
