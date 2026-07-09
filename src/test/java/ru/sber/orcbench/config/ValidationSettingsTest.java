package ru.sber.orcbench.config;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ValidationSettingsTest {

    @Test
    void parsesCustomChecksAndSampleFraction() {
        ValidationSettings settings = ValidationSettings.from(Map.of(
                "validation-checks", "row_count_parity,checksum_parity",
                "validation-sample-fraction", "0.05",
                "log-format-share-tolerance", "0.2"
        ));

        assertEquals(2, settings.checks().size());
        assertTrue(settings.checks().contains(ValidationCheck.ROW_COUNT_PARITY));
        assertEquals(0.05, settings.sampleFraction(), 0.0001);
        assertEquals(0.2, settings.logFormatShareTolerance(), 0.0001);
    }
}
