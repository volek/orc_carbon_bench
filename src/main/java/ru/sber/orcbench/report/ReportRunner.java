package ru.sber.orcbench.report;

import org.apache.hadoop.fs.FSDataOutputStream;
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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
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

    public static void run(
            SparkSession spark,
            ReportSettings settings,
            String reportsRawPath,
            String reportsValidationPath,
            String reportsSummaryPath
    ) {
        LOG.info("Building report name={} formats={} summaryPath={}",
                settings.reportName(), settings.formats(), reportsSummaryPath);

        List<Dataset<Row>> summaryParts = new ArrayList<>();

        loadParquet(spark, reportsRawPath).ifPresent(raw -> {
            LOG.info("Loaded benchmark raw data from {}", reportsRawPath);
            summaryParts.add(aggregateBenchmark(raw));
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
        try {
            return Optional.of(spark.read().option("recursiveFileLookup", "false").parquet(path));
        } catch (Exception ex) {
            LOG.warn("Failed to read parquet at {}: {}", path, ex.getMessage());
            return Optional.empty();
        }
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

    private static boolean hasColumn(Dataset<Row> raw, String name) {
        return Arrays.asList(raw.columns()).contains(name);
    }

    private static Dataset<Row> aggregateBenchmark(Dataset<Row> raw) {
        Dataset<Row> prepared = withSparkVersion(withRuntime(raw));
        Dataset<Row> measured = hasColumn(prepared, "warmup")
                ? prepared.filter(col("warmup").equalTo(false))
                : prepared;

        Column sparkVersionAgg = max("spark_version").alias("spark_version");

        return measured.groupBy(
                        lit("benchmark").alias("source"),
                        col("scenario"),
                        col("format"),
                        col("spark_runtime")
                )
                .agg(
                        count(lit(1)).alias("runs"),
                        avg("duration_ms").alias("avg_duration_ms"),
                        expr("percentile_approx(duration_ms, 0.5)").alias("p50_duration_ms"),
                        expr("percentile_approx(duration_ms, 0.95)").alias("p95_duration_ms"),
                        min("duration_ms").alias("min_duration_ms"),
                        max("duration_ms").alias("max_duration_ms"),
                        avg("selectivity").alias("avg_selectivity"),
                        sparkVersionAgg
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
                lit(null).cast("long").alias("min_duration_ms"),
                lit(null).cast("long").alias("max_duration_ms"),
                lit(null).cast("double").alias("avg_selectivity"),
                col("passed"),
                col("spark_runtime"),
                col("spark_version")
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
