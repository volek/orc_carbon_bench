package ru.sber.orcbench.config;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ValidationSettingsTest {

    @Test
    void parsesCustomChecksAndSampleFraction() {
        Map<String, String> args = new HashMap<>();
        args.put("validation-checks", "row_count,timestamp_range");
        args.put("validation-sample-fraction", "0.05");
        args.put("log-format-share-tolerance", "0.2");
        ValidationSettings settings = ValidationSettings.from(args);

        assertEquals(2, settings.checks().size());
        assertTrue(settings.checks().contains(ValidationCheck.ROW_COUNT));
        assertEquals(0.05, settings.sampleFraction(), 0.0001);
        assertEquals(0.2, settings.logFormatShareTolerance(), 0.0001);
    }
}
