package ru.sber.orcbench.writer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sber.orcbench.benchmark.IndexBuildMetric;
import ru.sber.orcbench.config.CarbonWriteSettings;
import ru.sber.orcbench.config.IndexProfile;

import java.util.Locale;

public final class CarbonWriter {
    private static final Logger LOG = LoggerFactory.getLogger(CarbonWriter.class);

    private CarbonWriter() {
    }

    public static void write(
            SparkSession spark,
            Dataset<Row> dataset,
            String carbonPath,
            CarbonWriteSettings settings,
            String saveMode
    ) {
        Dataset<Row> toWrite = settings.hasExplicitWritePartitions()
                ? dataset.repartition(settings.writePartitions())
                : dataset;

        LOG.info(
                "Writing CarbonData: path={} table={} mode={} compression={} partitionBy={}",
                carbonPath,
                settings.tableName(),
                saveMode,
                settings.compression(),
                String.join(",", settings.partitionBy())
        );

        toWrite.write()
                .format("carbondata")
                .mode(saveMode)
                .option("tableName", settings.tableName())
                .option("compression", settings.compression())
                .partitionBy(settings.partitionBy())
                .save(carbonPath);
    }

    public static void createIndexes(SparkSession spark, CarbonWriteSettings settings) {
        createIndexesWithMetrics(spark, settings, "n/a", null);
    }

    public static java.util.List<IndexBuildMetric> createIndexesWithMetrics(
            SparkSession spark,
            CarbonWriteSettings settings,
            String runId,
            IndexProfile profile
    ) {
        java.util.List<IndexBuildMetric> metrics = new java.util.ArrayList<>();
        String tableName = settings.tableName();

        if (settings.enableBloomIndex()) {
            for (String column : settings.bloomIndexColumns()) {
                String indexName = sanitizeIndexName("bloom_" + column);
                String ddl = "CREATE INDEX IF NOT EXISTS " + indexName
                        + " ON TABLE " + tableName
                        + " (" + column + ") AS 'BLOOMFILTER'";
                long startedAt = System.nanoTime();
                LOG.info("Creating Bloom index: {}", ddl);
                spark.sql(ddl);
                long buildTimeMs = (System.nanoTime() - startedAt) / 1_000_000L;
                if (profile != null) {
                    metrics.add(new IndexBuildMetric(
                            runId, profile, indexName, "BLOOMFILTER", column, buildTimeMs, java.time.Instant.now()
                    ));
                }
            }
        }

        if (settings.enableLuceneIndex()) {
            for (String column : settings.luceneIndexColumns()) {
                String indexName = sanitizeIndexName("lucene_" + column);
                String ddl = "CREATE INDEX IF NOT EXISTS " + indexName
                        + " ON TABLE " + tableName
                        + " (" + column + ") AS 'LUCENE'";
                long startedAt = System.nanoTime();
                LOG.info("Creating Lucene index: {}", ddl);
                spark.sql(ddl);
                long buildTimeMs = (System.nanoTime() - startedAt) / 1_000_000L;
                if (profile != null) {
                    metrics.add(new IndexBuildMetric(
                            runId, profile, indexName, "LUCENE", column, buildTimeMs, java.time.Instant.now()
                    ));
                }
            }
        }

        return metrics;
    }

    private static String sanitizeIndexName(String raw) {
        return raw.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_]", "_");
    }
}
