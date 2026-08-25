package ru.sber.orcbench;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sber.orcbench.benchmark.BenchmarkRunner;
import ru.sber.orcbench.config.AppConfig;
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
                    "Starting mode={} sparkVersion={} sparkRuntime={} basePath={} orcPath={} reportsPath={}",
                    config.mode(),
                    runtime.sparkVersion(),
                    runtime.sparkRuntime(),
                    config.basePath(),
                    config.orcPath(),
                    config.reportsPath()
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
                            config.reportsRawPath(),
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
