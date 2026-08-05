package ru.sber.orcbench.benchmark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sber.orcbench.config.BenchmarkScenario;
import ru.sber.orcbench.config.CarbonWriteSettings;
import ru.sber.orcbench.config.IndexExperimentSettings;
import ru.sber.orcbench.config.IndexProfile;
import ru.sber.orcbench.config.OutputFormat;
import ru.sber.orcbench.generator.LogFormatType;
import ru.sber.orcbench.writer.CarbonWriter;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public final class IndexExperimentRunner {
    private static final Logger LOG = LoggerFactory.getLogger(IndexExperimentRunner.class);

    private static final Set<BenchmarkScenario> INDEX_SCENARIOS = EnumSet.of(
            BenchmarkScenario.FILTER_HIGH_CARDINALITY,
            BenchmarkScenario.FILTER_MEDIUM_CARDINALITY,
            BenchmarkScenario.LUCENE_TEXT_SEARCH
    );

    private IndexExperimentRunner() {
    }

    public static void run(
            SparkSession spark,
            IndexExperimentSettings settings,
            CarbonWriteSettings carbonWrite,
            String orcPath,
            String defaultCarbonPath,
            String reportsIndexPath,
            String reportsIndexBuildPath,
            long seed,
            long timestampStartMs,
            long timestampEndMs
    ) {
        String runId = UUID.randomUUID().toString();
        List<IndexExperimentResult> results = new java.util.ArrayList<>();
        List<IndexBuildMetric> buildMetrics = new java.util.ArrayList<>();

        LOG.info(
                "Index experiment runId={} profiles={} rebuildIndexes={} orcPath={} reportsPath={}",
                runId,
                settings.profiles(),
                settings.rebuildIndexes(),
                orcPath,
                reportsIndexPath
        );

        Dataset<Row> orcDataset = DatasetLoader.load(spark, OutputFormat.ORC, orcPath, defaultCarbonPath).cache();
        long orcTotalRows = orcDataset.count();
        FilterContext filterContext = resolveFilterContext(orcDataset, timestampStartMs, timestampEndMs);

        LOG.info("ORC reference dataset rows={}", orcTotalRows);
        runOrcReference(spark, settings, results, runId, orcDataset, filterContext, orcTotalRows, seed);

        for (IndexProfile profile : settings.profiles()) {
            String carbonPath = settings.resolveCarbonPath(profile, defaultCarbonPath);
            CarbonWriteSettings profileSettings = profile.applyTo(carbonWrite);

            LOG.info("Carbon profile={} path={} bloom={} lucene={}",
                    profile.cliValue(), carbonPath, profileSettings.enableBloomIndex(), profileSettings.enableLuceneIndex());

            if (settings.rebuildIndexes() && carbonPath.equals(defaultCarbonPath)) {
                buildMetrics.addAll(CarbonWriter.createIndexesWithMetrics(
                        spark, profileSettings, runId, profile
                ));
            }

            Dataset<Row> carbonDataset = spark.read().format("carbondata").load(carbonPath).cache();
            long carbonTotalRows = carbonDataset.count();

            for (BenchmarkScenario scenario : INDEX_SCENARIOS) {
                if (scenario == BenchmarkScenario.LUCENE_TEXT_SEARCH && !profile.luceneEnabled()) {
                    continue;
                }
                runMeasuredScenario(
                        spark, settings, results, runId, carbonDataset, filterContext,
                        scenario, OutputFormat.CARBON, profile, null, carbonTotalRows, seed
                );
            }

            if (profile.luceneEnabled()) {
                for (String logFormat : LogFormatType.ALL_VALUES) {
                    runLuceneByLogFormat(
                            spark, settings, results, runId, carbonDataset, filterContext,
                            profile, logFormat, carbonTotalRows, seed
                    );
                }
            }

            carbonDataset.unpersist();
        }

        orcDataset.unpersist();

        writeExperimentResults(spark, results, reportsIndexPath);
        if (!buildMetrics.isEmpty()) {
            writeBuildMetrics(spark, buildMetrics, reportsIndexBuildPath);
        }

        LOG.info(
                "Index experiment completed: runId={} queryResults={} buildMetrics={}",
                runId, results.size(), buildMetrics.size()
        );
    }

    private static void runOrcReference(
            SparkSession spark,
            IndexExperimentSettings settings,
            List<IndexExperimentResult> results,
            String runId,
            Dataset<Row> orcDataset,
            FilterContext filterContext,
            long totalRows,
            long seed
    ) {
        for (BenchmarkScenario scenario : INDEX_SCENARIOS) {
            if (scenario == BenchmarkScenario.LUCENE_TEXT_SEARCH) {
                continue;
            }
            runMeasuredScenario(
                    spark, settings, results, runId, orcDataset, filterContext,
                    scenario, OutputFormat.ORC, null, null, totalRows, seed
            );
        }
    }

    private static void runLuceneByLogFormat(
            SparkSession spark,
            IndexExperimentSettings settings,
            List<IndexExperimentResult> results,
            String runId,
            Dataset<Row> dataset,
            FilterContext filterContext,
            IndexProfile profile,
            String logFormat,
            long totalRows,
            long seed
    ) {
        for (int i = 0; i < settings.warmupRuns(); i++) {
            executeLuceneByLogFormat(spark, dataset, filterContext, logFormat, settings.clearCacheBetweenRuns());
        }

        for (int runIndex = 0; runIndex < settings.repeatRuns(); runIndex++) {
            if (settings.clearCacheBetweenRuns()) {
                spark.catalog().clearCache();
            }
            long startedAt = System.nanoTime();
            long rowsReturned = executeLuceneByLogFormat(spark, dataset, filterContext, logFormat, false);
            long durationMs = (System.nanoTime() - startedAt) / 1_000_000L;

            results.add(IndexExperimentResult.of(
                    runId,
                    BenchmarkScenario.LUCENE_TEXT_SEARCH,
                    OutputFormat.CARBON,
                    profile,
                    logFormat,
                    runIndex,
                    durationMs,
                    rowsReturned,
                    totalRows,
                    seed
            ));
        }
    }

    private static void runMeasuredScenario(
            SparkSession spark,
            IndexExperimentSettings settings,
            List<IndexExperimentResult> results,
            String runId,
            Dataset<Row> dataset,
            FilterContext filterContext,
            BenchmarkScenario scenario,
            OutputFormat format,
            IndexProfile profile,
            String logFormat,
            long totalRows,
            long seed
    ) {
        for (int i = 0; i < settings.warmupRuns(); i++) {
            executeScenario(spark, dataset, scenario, filterContext, settings.clearCacheBetweenRuns());
        }

        for (int runIndex = 0; runIndex < settings.repeatRuns(); runIndex++) {
            if (settings.clearCacheBetweenRuns()) {
                spark.catalog().clearCache();
            }
            long startedAt = System.nanoTime();
            long rowsReturned = executeScenario(spark, dataset, scenario, filterContext, false);
            long durationMs = (System.nanoTime() - startedAt) / 1_000_000L;

            results.add(IndexExperimentResult.of(
                    runId,
                    scenario,
                    format,
                    profile,
                    logFormat,
                    runIndex,
                    durationMs,
                    rowsReturned,
                    totalRows,
                    seed
            ));

            LOG.info(
                    "Index experiment scenario={} format={} profile={} logFormat={} run={} durationMs={}",
                    scenario.cliValue(),
                    format.cliValue(),
                    profile == null ? "orc_reference" : profile.cliValue(),
                    logFormat == null ? "-" : logFormat,
                    runIndex,
                    durationMs
            );
        }
    }

    private static long executeLuceneByLogFormat(
            SparkSession spark,
            Dataset<Row> dataset,
            FilterContext filterContext,
            String logFormat,
            boolean clearCache
    ) {
        if (clearCache) {
            spark.catalog().clearCache();
        }
        return BenchmarkQueries.luceneTextSearchByLogFormat(dataset, filterContext, logFormat).count();
    }

    private static FilterContext resolveFilterContext(
            Dataset<Row> dataset,
            long timestampStartMs,
            long timestampEndMs
    ) {
        List<Row> sampleRows = dataset.limit(1).collectAsList();
        if (sampleRows.isEmpty()) {
            throw new IllegalStateException("Cannot run index experiments on empty dataset");
        }
        return FilterContext.fromSample(sampleRows.get(0), timestampStartMs, timestampEndMs);
    }

    private static long executeScenario(
            SparkSession spark,
            Dataset<Row> dataset,
            BenchmarkScenario scenario,
            FilterContext filterContext,
            boolean clearCache
    ) {
        if (clearCache) {
            spark.catalog().clearCache();
        }
        return BenchmarkQueries.apply(dataset, scenario, filterContext).count();
    }

    private static void writeExperimentResults(
            SparkSession spark,
            List<IndexExperimentResult> results,
            String outputPath
    ) {
        StructType schema = new StructType()
                .add("run_id", DataTypes.StringType, false)
                .add("scenario", DataTypes.StringType, false)
                .add("format", DataTypes.StringType, false)
                .add("index_profile", DataTypes.StringType, false)
                .add("log_format", DataTypes.StringType, true)
                .add("run_index", DataTypes.IntegerType, false)
                .add("duration_ms", DataTypes.LongType, false)
                .add("rows_returned", DataTypes.LongType, false)
                .add("total_rows", DataTypes.LongType, false)
                .add("selectivity", DataTypes.DoubleType, false)
                .add("seed", DataTypes.LongType, false)
                .add("executed_at", DataTypes.StringType, false);

        List<Row> rows = results.stream()
                .map(result -> org.apache.spark.sql.RowFactory.create(
                        result.runId(),
                        result.scenario().cliValue(),
                        result.format().cliValue(),
                        result.indexProfile() == null ? "orc_reference" : result.indexProfile().cliValue(),
                        result.logFormat(),
                        result.runIndex(),
                        result.durationMs(),
                        result.rowsReturned(),
                        result.totalRows(),
                        result.selectivity(),
                        result.seed(),
                        result.executedAt().toString()
                ))
                .collect(Collectors.toList());

        spark.createDataFrame(rows, schema)
                .coalesce(1)
                .write()
                .mode("overwrite")
                .parquet(outputPath);
    }

    private static void writeBuildMetrics(
            SparkSession spark,
            List<IndexBuildMetric> metrics,
            String outputPath
    ) {
        StructType schema = new StructType()
                .add("run_id", DataTypes.StringType, false)
                .add("index_profile", DataTypes.StringType, false)
                .add("index_name", DataTypes.StringType, false)
                .add("index_type", DataTypes.StringType, false)
                .add("column_name", DataTypes.StringType, false)
                .add("build_time_ms", DataTypes.LongType, false)
                .add("executed_at", DataTypes.StringType, false);

        List<Row> rows = metrics.stream()
                .map(metric -> org.apache.spark.sql.RowFactory.create(
                        metric.runId(),
                        metric.profile().cliValue(),
                        metric.indexName(),
                        metric.indexType(),
                        metric.columnName(),
                        metric.buildTimeMs(),
                        metric.executedAt().toString()
                ))
                .collect(Collectors.toList());

        spark.createDataFrame(rows, schema)
                .coalesce(1)
                .write()
                .mode("overwrite")
                .parquet(outputPath);
    }
}
