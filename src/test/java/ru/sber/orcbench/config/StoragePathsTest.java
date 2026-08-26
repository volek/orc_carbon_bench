package ru.sber.orcbench.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StoragePathsTest {

    @Test
    void derivesReportPathsFromBase() {
        StoragePaths paths = StoragePaths.from(
                "hdfs:///user/hdfs_migration_user/orc_test", null, null);
        assertEquals("hdfs:///user/hdfs_migration_user/orc_test/orc", paths.orcPath());
        assertEquals("hdfs:///user/hdfs_migration_user/orc_test/reports/raw", paths.reportsRawPath());
        assertEquals(
                "hdfs:///user/hdfs_migration_user/orc_test/reports/raw/benchmark",
                paths.reportsBenchmarkPath()
        );
        assertEquals(
                "hdfs:///user/hdfs_migration_user/orc_test/reports/raw/validation",
                paths.reportsValidationPath()
        );
        assertEquals("hdfs:///user/hdfs_migration_user/orc_test/reports/summary", paths.reportsSummaryPath());
    }
}
