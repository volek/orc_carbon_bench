package ru.sber.orcbench;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sber.orcbench.benchmark.BenchmarkRunner;
import ru.sber.orcbench.config.AppConfig;
import ru.sber.orcbench.config.OrcWriteSettings;
import ru.sber.orcbench.config.SparkRuntimeInfo;
import ru.sber.orcbench.generator.GenerateRunner;
import ru.sber.orcbench.generator.GeneratorConfig;
import ru.sber.orcbench.report.ReportRunner;
import ru.sber.orcbench.validation.ValidationRunner;
import ru.sber.orcbench.writer.SparkConfigurator;

public final class AppMain {
    private static final Logger LOG = LoggerFactory.getLogger(AppMain.class);

    private AppMain() {
    }

    public static void main(String[] args) {
        AppConfig config = AppConfig.fromArgs(args);

        SparkSession spark = SparkConfigurator.configureBuilder(
                SparkSession.builder().appName("orc-bench"),
                config
        ).getOrCreate();
        SparkConfigurator.configure(spark, config);
        SparkRuntimeInfo runtime = SparkRuntimeInfo.from(spark);
        try {
            LOG.info(
                    "Starting mode={} sparkVersion={} sparkRuntime={} basePath={} orcPath={} reportsPath={} bloom={}",
                    config.mode(),
                    runtime.sparkVersion(),
                    runtime.sparkRuntime(),
                    config.basePath(),
                    config.orcPath(),
                    config.reportsPath(),
                    config.orcWrite()
            );

            switch (config.mode()) {
                case GENERATE:
                    GenerateRunner.run(spark, GeneratorConfig.from(config));
                    break;
                case VALIDATE:
                    ValidationRunner.run(
                            spark,
                            config.validation(),
                            config.orcWrite(),
                            config.orcPath(),
                            config.reportsValidationPath(),
                            config.seed(),
                            config.timestampStartEpochMs(),
                            config.timestampEndEpochMs(),
                            runtime
                    );
                    break;
                case BENCHMARK:
                    BenchmarkRunner.run(
                            spark,
                            config.benchmark(),
                            config.orcPath(),
                            config.reportsBenchmarkPath(),
                            config.seed(),
                            config.timestampStartEpochMs(),
                            config.timestampEndEpochMs(),
                            config.benchmarkDatasetLabel(),
                            resolveOrcBloomColumns(config),
                            runtime
                    );
                    break;
                case REPORT:
                    ReportRunner.run(
                            spark,
                            config.report(),
                            config.paths(),
                            config.reportsSummaryPath()
                    );
                    break;
                default:
                    throw new IllegalStateException("Unsupported mode: " + config.mode());
            }
        } finally {
            spark.stop();
        }
    }

    static String resolveOrcBloomColumns(AppConfig config) {
        if ("bloom".equals(config.benchmarkDatasetLabel())) {
            return String.join(",", OrcWriteSettings.DEFAULT_BLOOM_FILTER_COLUMNS);
        }
        if ("nobloom".equals(config.benchmarkDatasetLabel())) {
            return "none";
        }
        return config.orcWrite().bloomFiltersEnabled()
                ? config.orcWrite().bloomFilterColumnsCsv()
                : "none";
    }
}
