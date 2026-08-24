package ru.sber.orcbench.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StoragePathsTest {

    @Test
    void derivesSpark32OrcReportPathFromReportsRoot() {
        StoragePaths paths = StoragePaths.from(
                "hdfs:///user/hdfs_migration_user/carbon_test", null, null, null);
        assertEquals("hdfs:///user/hdfs_migration_user/carbon_test/orc", paths.orcPath());
        assertEquals("hdfs:///user/hdfs_migration_user/carbon_test/carbon", paths.carbonPath());
        assertEquals("hdfs:///user/hdfs_migration_user/carbon_test/reports/raw", paths.reportsRawPath());
        assertEquals(
                "hdfs:///user/hdfs_migration_user/carbon_test/reports/raw/spark32-orc",
                paths.reportsSpark32OrcPath()
        );
        assertEquals("hdfs:///user/hdfs_migration_user/carbon_test/reports/raw/index", paths.reportsIndexPath());
        assertEquals(
                "hdfs:///user/hdfs_migration_user/carbon_test/reports/raw/validation",
                paths.reportsValidationPath()
        );
    }
}
