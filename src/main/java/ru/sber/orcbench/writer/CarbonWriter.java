package ru.sber.orcbench.writer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sber.orcbench.benchmark.IndexBuildMetric;
import ru.sber.orcbench.config.CarbonWriteSettings;
import ru.sber.orcbench.config.IndexProfile;
import ru.sber.orcbench.config.SparkRuntimeInfo;

import java.util.Locale;

public final class CarbonWriter {
    private static final Logger LOG = LoggerFactory.getLogger(CarbonWriter.class);

    private CarbonWriter() {
    }

    static final String SQL_EXTENSIONS = "org.apache.spark.sql.CarbonExtensions";
    static final String SESSION_STATE_BUILDER = "org.apache.spark.sql.hive.CarbonSessionStateBuilder";

    public static SparkSession.Builder configureBuilder(SparkSession.Builder builder) {
        return builder
                .enableHiveSupport()
                .config("spark.sql.extensions", SQL_EXTENSIONS)
                .config("spark.sql.session.state.builder", SESSION_STATE_BUILDER);
    }

    /**
     * {@code spark.sql.extensions} is static in Spark 3.1+ cluster sessions: it cannot be changed after
     * {@code SparkSession} exists. Fail with a submit hint instead of {@code AnalysisException}.
     */
    public static void requireConfigured(SparkSession spark) {
        String extensions = spark.conf().get("spark.sql.extensions", "");
        if (extensions == null || !extensions.contains("CarbonExtensions")) {
            throw new IllegalStateException(
                    "spark.sql.extensions is a static Spark config and was not set to CarbonExtensions "
                            + "before SparkSession creation. Re-submit with:\n"
                            + "  --conf spark.sql.extensions=" + SQL_EXTENSIONS + "\n"
                            + "  --conf spark.sql.session.state.builder=" + SESSION_STATE_BUILDER
            );
        }

        String catalog = spark.conf().get("spark.sql.catalog.spark_catalog", "");
        if (catalog != null && catalog.contains("CarbonSessionCatalog")) {
            throw new IllegalStateException(
                    "Invalid spark.sql.catalog.spark_catalog=" + catalog + ". "
                            + "CarbonData 2.3 does not use a V2 catalog plugin — remove that --conf. "
                            + "Use instead:\n"
                            + "  --conf spark.sql.extensions=" + SQL_EXTENSIONS + "\n"
                            + "  --conf spark.sql.session.state.builder=" + SESSION_STATE_BUILDER
            );
        }

        String stateBuilder = spark.conf().get("spark.sql.session.state.builder", "");
        if (stateBuilder == null || !stateBuilder.contains("CarbonSessionStateBuilder")) {
            throw new IllegalStateException(
                    "spark.sql.session.state.builder was not set to CarbonSessionStateBuilder "
                            + "before SparkSession creation. Re-submit with:\n"
                            + "  --conf spark.sql.session.state.builder=" + SESSION_STATE_BUILDER
            );
        }
    }

    public static void write(
            SparkSession spark,
            Dataset<Row> dataset,
            String carbonPath,
            CarbonWriteSettings settings,
            String saveMode
    ) {
        requireConfigured(spark);

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
                    metrics.add(IndexBuildMetric.of(
                            runId, profile, indexName, "BLOOMFILTER", column, buildTimeMs,
                            SparkRuntimeInfo.from(spark)
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
                    metrics.add(IndexBuildMetric.of(
                            runId, profile, indexName, "LUCENE", column, buildTimeMs,
                            SparkRuntimeInfo.from(spark)
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
