package ru.sber.orcbench.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StoragePathsTest {

    @Test
    void derivesReportPathsFromBase() {
        StoragePaths paths = StoragePaths.from(
                "hdfs:///user/hdfs_migration_user/orc_test", null, null, null, null);
        assertEquals("hdfs:///user/hdfs_migration_user/orc_test/orc", paths.orcPath());
        assertEquals("hdfs:///user/hdfs_migration_user/orc_test/orc_bloom", paths.orcBloomPath());
        assertEquals("hdfs:///user/hdfs_migration_user/orc_test/reports/raw", paths.reportsRawPath());
        assertEquals(
                "hdfs:///user/hdfs_migration_user/orc_test/reports/raw/benchmark",
                paths.reportsBenchmarkPath()
        );
        assertEquals(
                "hdfs:///user/hdfs_migration_user/orc_test/reports/raw/benchmark_nobloom",
                paths.reportsBenchmarkNobloomPath()
        );
        assertEquals(
                "hdfs:///user/hdfs_migration_user/orc_test/reports/raw/benchmark_bloom",
                paths.reportsBenchmarkBloomPath()
        );
        assertEquals(
                "hdfs:///user/hdfs_migration_user/orc_test/reports/raw/validation",
                paths.reportsValidationPath()
        );
        assertEquals("hdfs:///user/hdfs_migration_user/orc_test/reports/summary", paths.reportsSummaryPath());
        assertEquals("hdfs:///user/hdfs_migration_user/orc_test/dictionary", paths.dictionaryPath());
        assertEquals("default", paths.layoutId());
    }

    @Test
    void derivesLayoutScopedPaths() {
        StoragePaths paths = StoragePaths.from(
                "hdfs:///base", null, null, null, null, null, "b0");
        assertEquals("hdfs:///base/layouts/b0/orc", paths.orcPath());
        assertEquals("hdfs:///base/layouts/b0/dictionary", paths.dictionaryPath());
        assertEquals("hdfs:///base/layouts/b0/reports/raw/benchmark", paths.reportsBenchmarkPath());
        assertEquals("b0", paths.layoutId());
    }

    @Test
    void allowsCustomBenchmarkAndValidationPaths() {
        StoragePaths paths = StoragePaths.from(
                "hdfs:///base",
                "hdfs:///base/orc_custom",
                null,
                "hdfs:///base/reports/raw/benchmark_custom",
                "hdfs:///base/reports/raw/validation_custom"
        );
        assertEquals("hdfs:///base/orc_custom", paths.orcPath());
        assertEquals("hdfs:///base/reports/raw/benchmark_custom", paths.reportsBenchmarkPath());
        assertEquals("hdfs:///base/reports/raw/validation_custom", paths.reportsValidationPath());
    }
}
