package ru.sber.orcbench;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sber.orcbench.benchmark.BenchmarkRunner;
import ru.sber.orcbench.benchmark.IndexExperimentRunner;
import ru.sber.orcbench.config.AppConfig;
import ru.sber.orcbench.config.SparkRuntime;
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
        SparkRuntime.requireCompatible(config);

        SparkSession spark = SparkConfigurator.configureBuilder(
                SparkSession.builder().appName("orc-carbon-bench"),
                config
        ).getOrCreate();
        SparkConfigurator.configure(spark, config);
        SparkRuntimeInfo runtime = SparkRuntimeInfo.from(spark);
        try {
            LOG.info(
                    "Starting mode={} sparkVersion={} sparkRuntime={} basePath={} orcPath={} carbonPath={} "
                            + "reportsPath={} outputFormats={}",
                    config.mode(),
                    runtime.sparkVersion(),
                    runtime.sparkRuntime(),
                    config.basePath(),
                    config.orcPath(),
                    config.carbonPath(),
                    config.reportsPath(),
                    config.outputFormats()
            );

            switch (config.mode()) {
                case GENERATE:
                    GenerateRunner.run(spark, GeneratorConfig.from(config));
                    break;
                case VALIDATE:
                    ValidationRunner.run(
                            spark,
                            config.validation(),
                            config.orcPath(),
                            config.carbonPath(),
                            config.reportsValidationPath(),
                            config.seed(),
                            config.timestampStartEpochMs(),
                            config.timestampEndEpochMs(),
                            runtime
                    );
                    break;
                case INDEX_EXPERIMENT:
                    IndexExperimentRunner.run(
                            spark,
                            config.indexExperiment(),
                            config.carbonWrite(),
                            config.orcPath(),
                            config.carbonPath(),
                            config.reportsIndexPath(),
                            config.reportsIndexBuildPath(),
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
                            config.carbonPath(),
                            SparkRuntime.benchmarkOutputPath(config),
                            config.seed(),
                            config.timestampStartEpochMs(),
                            config.timestampEndEpochMs(),
                            runtime
                    );
                    break;
                case REPORT:
                    ReportRunner.run(
                            spark,
                            config.report(),
                            config.reportsRawPath(),
                            config.reportsSpark32OrcPath(),
                            config.reportsIndexPath(),
                            config.reportsIndexBuildPath(),
                            config.reportsValidationPath(),
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
}
