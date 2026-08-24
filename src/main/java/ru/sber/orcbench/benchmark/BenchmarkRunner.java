package ru.sber.orcbench.benchmark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sber.orcbench.config.BenchmarkScenario;
import ru.sber.orcbench.config.BenchmarkSettings;
import ru.sber.orcbench.config.OutputFormat;
import ru.sber.orcbench.config.SparkRuntimeInfo;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public final class BenchmarkRunner {
    private static final Logger LOG = LoggerFactory.getLogger(BenchmarkRunner.class);

    private BenchmarkRunner() {
    }

    public static void run(
            SparkSession spark,
            BenchmarkSettings settings,
            String orcPath,
            String carbonPath,
            String reportsRawPath,
            long seed,
            long timestampStartMs,
            long timestampEndMs,
            SparkRuntimeInfo runtime
    ) {
        String runId = UUID.randomUUID().toString();
        List<BenchmarkResult> results = new java.util.ArrayList<>();

        LOG.info(
                "Benchmark runId={} formats={} scenarios={} warmupRuns={} repeatRuns={} clearCache={} reportsPath={}",
                runId,
                settings.formats(),
                settings.scenarios(),
                settings.warmupRuns(),
                settings.repeatRuns(),
                settings.clearCacheBetweenRuns(),
                reportsRawPath
        );

        for (OutputFormat format : settings.formats()) {
            String dataPath = format == OutputFormat.ORC ? orcPath : carbonPath;
            LOG.info("Loading dataset format={} path={}", format, dataPath);

            Dataset<Row> dataset = DatasetLoader.load(spark, format, orcPath, carbonPath).cache();
            long totalRows = dataset.count();
            FilterContext filterContext = resolveFilterContext(dataset, seed, timestampStartMs, timestampEndMs);

            LOG.info("Dataset ready format={} totalRows={} sampleEventId={}", format, totalRows, filterContext.eventId());

            for (BenchmarkScenario scenario : settings.scenarios()) {
                if (scenario == BenchmarkScenario.LUCENE_TEXT_SEARCH && format != OutputFormat.CARBON) {
                    LOG.info("Skipping scenario={} for format={} (CarbonData only)", scenario, format);
                    continue;
                }

                LOG.info("Running scenario={} format={}", scenario, format);

                for (int i = 0; i < settings.warmupRuns(); i++) {
                    executeScenario(spark, dataset, scenario, filterContext, settings.clearCacheBetweenRuns());
                }

                for (int runIndex = 0; runIndex < settings.repeatRuns(); runIndex++) {
                    if (settings.clearCacheBetweenRuns()) {
                        dataset.unpersist();
                        spark.catalog().clearCache();
                        dataset = DatasetLoader.load(spark, format, orcPath, carbonPath).cache();
                    }

                    long startedAt = System.nanoTime();
                    long rowsReturned = executeScenario(spark, dataset, scenario, filterContext, false);
                    long durationMs = (System.nanoTime() - startedAt) / 1_000_000L;

                    BenchmarkResult result = BenchmarkResult.of(
                            runId,
                            scenario,
                            format,
                            runIndex,
                            false,
                            durationMs,
                            rowsReturned,
                            totalRows,
                            seed,
                            runtime
                    );
                    results.add(result);

                    LOG.info(
                            "Measured scenario={} format={} run={} durationMs={} rowsReturned={} selectivity={}",
                            scenario.cliValue(),
                            format.cliValue(),
                            runIndex,
                            durationMs,
                            rowsReturned,
                            result.selectivity()
                    );
                }
            }

            dataset.unpersist();
        }

        writeResults(spark, results, reportsRawPath);
        LOG.info("Benchmark completed: runId={} results={} output={}", runId, results.size(), reportsRawPath);
    }

    private static FilterContext resolveFilterContext(
            Dataset<Row> dataset,
            long seed,
            long timestampStartMs,
            long timestampEndMs
    ) {
        List<Row> sampleRows = dataset.limit(1).collectAsList();
        if (sampleRows.isEmpty()) {
            throw new IllegalStateException("Cannot run benchmarks on empty dataset");
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

        Dataset<Row> query = BenchmarkQueries.apply(dataset, scenario, filterContext);
        return query.count();
    }

    private static void writeResults(SparkSession spark, List<BenchmarkResult> results, String reportsRawPath) {
        StructType schema = new StructType()
                .add("run_id", DataTypes.StringType, false)
                .add("scenario", DataTypes.StringType, false)
                .add("format", DataTypes.StringType, false)
                .add("run_index", DataTypes.IntegerType, false)
                .add("warmup", DataTypes.BooleanType, false)
                .add("duration_ms", DataTypes.LongType, false)
                .add("rows_returned", DataTypes.LongType, false)
                .add("total_rows", DataTypes.LongType, false)
                .add("selectivity", DataTypes.DoubleType, false)
                .add("seed", DataTypes.LongType, false)
                .add("executed_at", DataTypes.StringType, false)
                .add("spark_version", DataTypes.StringType, false)
                .add("spark_runtime", DataTypes.StringType, false);

        List<Row> rows = results.stream()
                .map(result -> org.apache.spark.sql.RowFactory.create(
                        result.runId(),
                        result.scenario().cliValue(),
                        result.format().cliValue(),
                        result.runIndex(),
                        result.warmup(),
                        result.durationMs(),
                        result.rowsReturned(),
                        result.totalRows(),
                        result.selectivity(),
                        result.seed(),
                        result.executedAt().toString(),
                        result.sparkVersion(),
                        result.sparkRuntime()
                ))
                .collect(Collectors.toList());

        spark.createDataFrame(rows, schema)
                .coalesce(1)
                .write()
                .mode("overwrite")
                .parquet(reportsRawPath);
    }
}
