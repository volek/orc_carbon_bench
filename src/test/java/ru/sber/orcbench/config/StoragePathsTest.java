package ru.sber.orcbench.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StoragePathsTest {

    @Test
    void derivesSpark32OrcReportPathFromReportsRoot() {
        StoragePaths paths = StoragePaths.from("/bench/orc-carbon", null, null, null);
        assertEquals("/bench/orc-carbon/orc", paths.orcPath());
        assertEquals("/bench/orc-carbon/carbon", paths.carbonPath());
        assertEquals("/bench/orc-carbon/reports/raw", paths.reportsRawPath());
        assertEquals("/bench/orc-carbon/reports/raw/spark32-orc", paths.reportsSpark32OrcPath());
        assertEquals("/bench/orc-carbon/reports/raw/index", paths.reportsIndexPath());
        assertEquals("/bench/orc-carbon/reports/raw/validation", paths.reportsValidationPath());
    }
}
