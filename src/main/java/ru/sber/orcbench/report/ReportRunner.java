package ru.sber.orcbench.report;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sber.orcbench.config.ReportFormat;
import ru.sber.orcbench.config.ReportSettings;
import ru.sber.orcbench.config.SparkRuntime;
import ru.sber.orcbench.config.StoragePaths;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;

public final class ReportRunner {
    private static final Logger LOG = LoggerFactory.getLogger(ReportRunner.class);

    private ReportRunner() {
    }

    public static void run(
            SparkSession spark,
            ReportSettings settings,
            StoragePaths paths,
            String reportsSummaryPath
    ) {
        LOG.info("Building report name={} formats={} summaryPath={}",
                settings.reportName(), settings.formats(), reportsSummaryPath);

        List<Dataset<Row>> summaryParts = new ArrayList<>();

        loadAllBenchmark(spark, paths).ifPresent(raw -> {
            LOG.info("Loaded benchmark raw data rows={}", raw.count());
            summaryParts.add(aggregateBenchmark(raw));
        });

        loadAllValidation(spark, paths).ifPresent(raw -> {
            LOG.info("Loaded validation results");
            summaryParts.add(aggregateValidation(raw));
        });

        if (summaryParts.isEmpty()) {
            throw new IllegalStateException(
                    "No report input data found under " + paths.reportsRawPath()
                            + ". Expected parquet datasets in subdirectories such as "
                            + "benchmark/, validation/, benchmark_nobloom/, benchmark_bloom/, "
                            + "layouts/*/reports/raw/benchmark*/, "
                            + "validation_nobloom/, validation_bloom/. "
                            + "Run --mode=validate and --mode=benchmark for this --base-path "
                            + "before --mode=report."
            );
        }

        Dataset<Row> summary = summaryParts.get(0);
        for (int i = 1; i < summaryParts.size(); i++) {
            summary = summary.unionByName(summaryParts.get(i), true);
        }

        String dataOutputBase = reportsSummaryPath + "/results";
        String markdownOutput = reportsSummaryPath + "/" + settings.reportName() + ".md";

        if (settings.formats().contains(ReportFormat.PARQUET)) {
            writeParquet(summary, dataOutputBase + ".parquet");
        }
        if (settings.formats().contains(ReportFormat.CSV)) {
            writeCsv(summary, dataOutputBase + ".csv");
        }
        if (settings.formats().contains(ReportFormat.JSON)) {
            writeJson(summary, dataOutputBase + ".json");
        }
        if (settings.formats().contains(ReportFormat.MARKDOWN)) {
            String markdown = MarkdownReportBuilder.build(summary, settings.reportName());
            writeMarkdown(spark, markdown, markdownOutput);
        }

        LOG.info("Report generation completed: summaryPath={}", reportsSummaryPath);
    }

    private static Optional<Dataset<Row>> loadAllBenchmark(SparkSession spark, StoragePaths paths) {
        return combineMatching(spark, discoverInputPaths(spark, paths), true);
    }

    private static Optional<Dataset<Row>> loadAllValidation(SparkSession spark, StoragePaths paths) {
        return combineMatching(spark, discoverInputPaths(spark, paths), false);
    }

    /**
     * Hardcoded A/B and default locations, plus any parquet dataset sitting
     * directly under {@code reports/raw/} (or in {@code raw/} itself for legacy writes).
     */
    private static Set<String> discoverInputPaths(SparkSession spark, StoragePaths paths) {
        Set<String> candidatePaths = new LinkedHashSet<>();
        addQualified(spark, candidatePaths, paths.reportsBenchmarkNobloomPath());
        addQualified(spark, candidatePaths, paths.reportsBenchmarkBloomPath());
        addQualified(spark, candidatePaths, paths.reportsBenchmarkPath());
        addQualified(spark, candidatePaths, paths.reportsValidationNobloomPath());
        addQualified(spark, candidatePaths, paths.reportsValidationBloomPath());
        addQualified(spark, candidatePaths, paths.reportsValidationPath());
        candidatePaths.addAll(listParquetDatasets(spark, paths.reportsRawPath()));
        // Factor layouts live under <base>/layouts/*/reports/raw/
        candidatePaths.addAll(listParquetDatasetsRecursive(spark, paths.basePath() + "/layouts", 3));
        return candidatePaths;
    }

    private static void addQualified(SparkSession spark, Set<String> paths, String path) {
        paths.add(qualify(spark, path));
    }

    private static String qualify(SparkSession spark, String path) {
        try {
            Path hadoopPath = new Path(path);
            FileSystem fs = hadoopPath.getFileSystem(spark.sparkContext().hadoopConfiguration());
            return fs.makeQualified(hadoopPath).toString();
        } catch (Exception ex) {
            return path;
        }
    }

    private static Optional<Dataset<Row>> combineMatching(
            SparkSession spark,
            Set<String> candidatePaths,
            boolean benchmark
    ) {
        Dataset<Row> combined = null;
        for (String path : candidatePaths) {
            Optional<Dataset<Row>> loaded = loadParquet(spark, path);
            if (!loaded.isPresent()) {
                continue;
            }
            Dataset<Row> dataset = loaded.get();
            if (benchmark && !isBenchmarkDataset(dataset)) {
                continue;
            }
            if (!benchmark && !isValidationDataset(dataset)) {
                continue;
            }
            LOG.info("Loaded {} metrics from {}", benchmark ? "benchmark" : "validation", path);
            combined = combined == null ? dataset : combined.unionByName(dataset, true);
        }
        return Optional.ofNullable(combined);
    }

    static boolean isBenchmarkDataset(Dataset<Row> dataset) {
        return hasColumn(dataset, "duration_ms")
                && hasColumn(dataset, "scenario")
                && hasColumn(dataset, "format");
    }

    static boolean isValidationDataset(Dataset<Row> dataset) {
        return hasColumn(dataset, "check") && hasColumn(dataset, "passed");
    }

    /**
     * Try Spark's parquet reader directly. Do not gate on {@code FileSystem.exists}:
     * {@code hdfs:///} URIs can resolve differently for a raw Hadoop FS client than
     * for Spark SQL, which produced false negatives on the Pilot cluster.
     */
    private static Optional<Dataset<Row>> loadParquet(SparkSession spark, String path) {
        try {
            return Optional.of(spark.read().parquet(path));
        } catch (Exception ex) {
            LOG.warn("Failed to read parquet at {}: {}", path, ex.getMessage());
            return Optional.empty();
        }
    }

    private static Set<String> listParquetDatasets(SparkSession spark, String rawPath) {
        return listParquetDatasetsRecursive(spark, rawPath, 1);
    }

    private static Set<String> listParquetDatasetsRecursive(SparkSession spark, String rawPath, int maxDepth) {
        Set<String> datasets = new LinkedHashSet<>();
        try {
            Path root = new Path(rawPath);
            FileSystem fs = root.getFileSystem(spark.sparkContext().hadoopConfiguration());
            Path qualified = fs.makeQualified(root);
            if (!fs.exists(qualified)) {
                LOG.warn("Report input path not found: {}", qualified);
                return datasets;
            }
            collectParquetDatasets(fs, qualified, datasets, 0, maxDepth);
        } catch (Exception ex) {
            LOG.warn("Failed to list parquet datasets under {}: {}", rawPath, ex.getMessage());
        }
        return datasets;
    }

    private static void collectParquetDatasets(
            FileSystem fs,
            Path dir,
            Set<String> datasets,
            int depth,
            int maxDepth
    ) throws IOException {
        if (hasParquetFiles(fs, dir)) {
            datasets.add(fs.makeQualified(dir).toString());
        }
        if (depth >= maxDepth) {
            return;
        }
        FileStatus[] children;
        try {
            children = fs.listStatus(dir);
        } catch (IOException ex) {
            return;
        }
        for (FileStatus child : children) {
            if (!child.isDirectory()) {
                continue;
            }
            String name = child.getPath().getName();
            if (name.startsWith("_") || name.startsWith(".")) {
                continue;
            }
            collectParquetDatasets(fs, child.getPath(), datasets, depth + 1, maxDepth);
        }
    }

    private static boolean hasParquetFiles(FileSystem fs, Path dir) throws IOException {
        FileStatus[] statuses;
        try {
            statuses = fs.listStatus(dir);
        } catch (IOException ex) {
            return false;
        }
        for (FileStatus status : statuses) {
            if (status.isFile() && status.getPath().getName().endsWith(".parquet")) {
                return true;
            }
        }
        return false;
    }

    private static Dataset<Row> withRuntime(Dataset<Row> raw) {
        if (hasColumn(raw, "spark_runtime")) {
            return raw;
        }
        return raw.withColumn("spark_runtime", lit(SparkRuntime.SPARK32_ORC));
    }

    private static Dataset<Row> withSparkVersion(Dataset<Row> raw) {
        if (hasColumn(raw, "spark_version")) {
            return raw;
        }
        return raw.withColumn("spark_version", lit(null).cast("string"));
    }

    private static Dataset<Row> withDatasetLabel(Dataset<Row> raw) {
        if (hasColumn(raw, "dataset_label")) {
            return raw;
        }
        return raw.withColumn("dataset_label", lit("default"));
    }

    private static boolean hasColumn(Dataset<Row> raw, String name) {
        return Arrays.asList(raw.columns()).contains(name);
    }

    private static Dataset<Row> aggregateBenchmark(Dataset<Row> raw) {
        Dataset<Row> prepared = withDatasetLabel(withSparkVersion(withRuntime(raw)));
        Dataset<Row> measured = hasColumn(prepared, "warmup")
                ? prepared.filter(col("warmup").equalTo(false))
                : prepared;

        Column sparkVersionAgg = max("spark_version").alias("spark_version");
        Column avgBytes = hasColumn(measured, "bytes_read")
                ? avg("bytes_read").alias("avg_bytes_read")
                : lit(null).cast("double").alias("avg_bytes_read");
        Column avgRecords = hasColumn(measured, "records_read")
                ? avg("records_read").alias("avg_records_read")
                : lit(null).cast("double").alias("avg_records_read");
        Column bloomColumns = hasColumn(measured, "orc_bloom_columns")
                ? max("orc_bloom_columns").alias("orc_bloom_columns")
                : lit(null).cast("string").alias("orc_bloom_columns");
        Column avgScanRatio = hasColumn(measured, "scan_ratio")
                ? avg("scan_ratio").alias("avg_scan_ratio")
                : lit(null).cast("double").alias("avg_scan_ratio");
        Column slaSuccessRate = hasColumn(measured, "sla_ok")
                ? avg(expr("cast(sla_ok as double)")).alias("sla_success_rate")
                : lit(null).cast("double").alias("sla_success_rate");
        Column avgSecondsPerGb = hasColumn(measured, "seconds_per_gb")
                ? avg("seconds_per_gb").alias("avg_seconds_per_gb")
                : lit(null).cast("double").alias("avg_seconds_per_gb");
        Column queryCategory = hasColumn(measured, "query_category")
                ? max("query_category").alias("query_category")
                : lit(null).cast("string").alias("query_category");
        Column slaClassCol = hasColumn(measured, "sla_class")
                ? max("sla_class").alias("sla_class")
                : lit(null).cast("string").alias("sla_class");
        Column durationGroup = hasColumn(measured, "duration_group")
                ? max("duration_group").alias("duration_group")
                : lit(null).cast("string").alias("duration_group");
        Column rowsCapRate = hasColumn(measured, "rows_cap_ok")
                ? avg(expr("cast(rows_cap_ok as double)")).alias("rows_cap_ok_rate")
                : lit(null).cast("double").alias("rows_cap_ok_rate");
        Column p99Duration = expr("cast(percentile_approx(duration_ms, 0.99) as double)").alias("p99_duration_ms");

        return measured.groupBy(
                        lit("benchmark").alias("source"),
                        col("scenario"),
                        col("format"),
                        col("spark_runtime"),
                        col("dataset_label"),
                        hasColumn(measured, "layout_id") ? col("layout_id") : lit("default").alias("layout_id"),
                        hasColumn(measured, "engine") ? col("engine") : lit("spark").alias("engine"),
                        hasColumn(measured, "cache_state") ? col("cache_state") : lit("cold").alias("cache_state")
                )
                .agg(
                        count(lit(1)).alias("runs"),
                        avg("duration_ms").alias("avg_duration_ms"),
                        expr("cast(percentile_approx(duration_ms, 0.5) as double)").alias("p50_duration_ms"),
                        expr("cast(percentile_approx(duration_ms, 0.95) as double)").alias("p95_duration_ms"),
                        p99Duration,
                        min("duration_ms").alias("min_duration_ms"),
                        max("duration_ms").alias("max_duration_ms"),
                        avg("selectivity").alias("avg_selectivity"),
                        avgBytes,
                        avgRecords,
                        bloomColumns,
                        sparkVersionAgg,
                        avgScanRatio,
                        slaSuccessRate,
                        avgSecondsPerGb,
                        queryCategory,
                        slaClassCol,
                        durationGroup,
                        rowsCapRate
                )
                .withColumn("passed", lit(null).cast("boolean"));
    }

    private static Dataset<Row> aggregateValidation(Dataset<Row> raw) {
        Dataset<Row> prepared = withSparkVersion(withRuntime(raw));
        return prepared.select(
                lit("validation").alias("source"),
                col("check").alias("scenario"),
                lit("orc").alias("format"),
                lit(1).alias("runs"),
                lit(null).cast("double").alias("avg_duration_ms"),
                lit(null).cast("double").alias("p50_duration_ms"),
                lit(null).cast("double").alias("p95_duration_ms"),
                lit(null).cast("double").alias("p99_duration_ms"),
                lit(null).cast("long").alias("min_duration_ms"),
                lit(null).cast("long").alias("max_duration_ms"),
                lit(null).cast("double").alias("avg_selectivity"),
                lit(null).cast("double").alias("avg_bytes_read"),
                lit(null).cast("double").alias("avg_records_read"),
                lit(null).cast("string").alias("orc_bloom_columns"),
                col("passed"),
                col("spark_runtime"),
                col("spark_version"),
                lit("-").alias("dataset_label"),
                lit("default").alias("layout_id"),
                lit("spark").alias("engine"),
                lit("-").alias("cache_state"),
                lit(null).cast("double").alias("avg_scan_ratio"),
                lit(null).cast("double").alias("sla_success_rate"),
                lit(null).cast("double").alias("avg_seconds_per_gb"),
                lit(null).cast("string").alias("query_category"),
                lit(null).cast("string").alias("sla_class"),
                lit(null).cast("string").alias("duration_group"),
                lit(null).cast("double").alias("rows_cap_ok_rate")
        );
    }

    private static void writeParquet(Dataset<Row> summary, String outputPath) {
        summary.coalesce(1).write().mode("overwrite").parquet(outputPath);
    }

    private static void writeCsv(Dataset<Row> summary, String outputPath) {
        summary.coalesce(1).write().mode("overwrite").option("header", "true").csv(outputPath);
    }

    private static void writeJson(Dataset<Row> summary, String outputPath) {
        summary.coalesce(1).write().mode("overwrite").json(outputPath);
    }

    private static void writeMarkdown(SparkSession spark, String markdown, String outputPath) {
        try {
            Path hadoopPath = new Path(outputPath);
            FileSystem fs = FileSystem.get(hadoopPath.toUri(), spark.sparkContext().hadoopConfiguration());
            if (fs.exists(hadoopPath)) {
                fs.delete(hadoopPath, false);
            }
            try (FSDataOutputStream out = fs.create(hadoopPath, true)) {
                out.write(markdown.getBytes(StandardCharsets.UTF_8));
            }
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to write markdown report to " + outputPath, ex);
        }
    }
}
