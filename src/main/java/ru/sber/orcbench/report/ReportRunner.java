package ru.sber.orcbench.report;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sber.orcbench.config.ReportFormat;
import ru.sber.orcbench.config.ReportSettings;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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

    public static void run(SparkSession spark, ReportSettings settings, String reportsRawPath,
                           String reportsIndexPath, String reportsIndexBuildPath,
                           String reportsValidationPath, String reportsSummaryPath) {
        LOG.info("Building report name={} formats={} summaryPath={}",
                settings.reportName(), settings.formats(), reportsSummaryPath);

        List<Dataset<Row>> summaryParts = new ArrayList<>();

        loadParquet(spark, reportsRawPath).ifPresent(raw -> {
            LOG.info("Loaded benchmark raw data from {}", reportsRawPath);
            summaryParts.add(aggregateBenchmark(raw, "benchmark"));
        });

        loadParquet(spark, reportsIndexPath).ifPresent(raw -> {
            LOG.info("Loaded index experiment data from {}", reportsIndexPath);
            summaryParts.add(aggregateIndexExperiments(raw));
        });

        loadParquet(spark, reportsIndexBuildPath).ifPresent(raw -> {
            LOG.info("Loaded index build metrics from {}", reportsIndexBuildPath);
            summaryParts.add(aggregateIndexBuild(raw));
        });

        loadParquet(spark, reportsValidationPath).ifPresent(raw -> {
            LOG.info("Loaded validation results from {}", reportsValidationPath);
            summaryParts.add(aggregateValidation(raw));
        });

        if (summaryParts.isEmpty()) {
            throw new IllegalStateException("No report input data found under " + reportsRawPath);
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

    private static Optional<Dataset<Row>> loadParquet(SparkSession spark, String path) {
        if (!pathExists(spark, path)) {
            LOG.warn("Report input path not found: {}", path);
            return Optional.empty();
        }
        return Optional.of(spark.read().parquet(path));
    }

    private static boolean pathExists(SparkSession spark, String path) {
        try {
            Path hadoopPath = new Path(path);
            FileSystem fs = FileSystem.get(hadoopPath.toUri(), spark.sparkContext().hadoopConfiguration());
            return fs.exists(hadoopPath);
        } catch (IOException ex) {
            LOG.warn("Failed to check path {}: {}", path, ex.getMessage());
            return false;
        }
    }

    private static Dataset<Row> aggregateBenchmark(Dataset<Row> raw, String source) {
        Dataset<Row> measured = raw.columns().length > 0 && java.util.Arrays.asList(raw.columns()).contains("warmup")
                ? raw.filter(col("warmup").equalTo(false))
                : raw;

        return measured.groupBy(lit(source).alias("source"), col("scenario"), col("format"))
                .agg(
                        count(lit(1)).alias("runs"),
                        avg("duration_ms").alias("avg_duration_ms"),
                        expr("percentile_approx(duration_ms, 0.5)").alias("p50_duration_ms"),
                        expr("percentile_approx(duration_ms, 0.95)").alias("p95_duration_ms"),
                        min("duration_ms").alias("min_duration_ms"),
                        max("duration_ms").alias("max_duration_ms"),
                        avg("selectivity").alias("avg_selectivity")
                )
                .withColumn("index_profile", lit(null).cast("string"))
                .withColumn("log_format", lit(null).cast("string"))
                .withColumn("passed", lit(null).cast("boolean"));
    }

    private static Dataset<Row> aggregateIndexExperiments(Dataset<Row> raw) {
        return raw.groupBy(
                        lit("index_experiment").alias("source"),
                        col("scenario"),
                        col("format"),
                        col("index_profile"),
                        col("log_format")
                )
                .agg(
                        count(lit(1)).alias("runs"),
                        avg("duration_ms").alias("avg_duration_ms"),
                        expr("percentile_approx(duration_ms, 0.5)").alias("p50_duration_ms"),
                        expr("percentile_approx(duration_ms, 0.95)").alias("p95_duration_ms"),
                        min("duration_ms").alias("min_duration_ms"),
                        max("duration_ms").alias("max_duration_ms"),
                        avg("selectivity").alias("avg_selectivity")
                )
                .withColumn("passed", lit(null).cast("boolean"));
    }

    private static Dataset<Row> aggregateIndexBuild(Dataset<Row> raw) {
        return raw.groupBy(lit("index_build").alias("source"), col("index_profile"), col("index_type"), col("column_name"))
                .agg(
                        count(lit(1)).alias("runs"),
                        avg("build_time_ms").alias("avg_duration_ms"),
                        expr("percentile_approx(build_time_ms, 0.5)").alias("p50_duration_ms"),
                        expr("percentile_approx(build_time_ms, 0.95)").alias("p95_duration_ms"),
                        min("build_time_ms").alias("min_duration_ms"),
                        max("build_time_ms").alias("max_duration_ms")
                )
                .withColumn("scenario", col("column_name"))
                .withColumn("format", col("index_type"))
                .withColumn("avg_selectivity", lit(null).cast("double"))
                .withColumn("log_format", lit(null).cast("string"))
                .withColumn("passed", lit(null).cast("boolean"))
                .drop("column_name", "index_type");
    }

    private static Dataset<Row> aggregateValidation(Dataset<Row> raw) {
        return raw.select(
                lit("validation").alias("source"),
                col("check").alias("scenario"),
                lit("n/a").alias("format"),
                lit(1).alias("runs"),
                lit(null).cast("double").alias("avg_duration_ms"),
                lit(null).cast("double").alias("p50_duration_ms"),
                lit(null).cast("double").alias("p95_duration_ms"),
                lit(null).cast("long").alias("min_duration_ms"),
                lit(null).cast("long").alias("max_duration_ms"),
                lit(null).cast("double").alias("avg_selectivity"),
                lit(null).cast("string").alias("index_profile"),
                lit(null).cast("string").alias("log_format"),
                col("passed")
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
            try (var out = fs.create(hadoopPath, true)) {
                out.write(markdown.getBytes(StandardCharsets.UTF_8));
            }
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to write markdown report to " + outputPath, ex);
        }
    }
}
