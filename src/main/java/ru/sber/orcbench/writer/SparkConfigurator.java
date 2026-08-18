package ru.sber.orcbench.writer;

import org.apache.spark.sql.SparkSession;
import ru.sber.orcbench.config.AppConfig;
import ru.sber.orcbench.config.Mode;
import ru.sber.orcbench.config.OutputFormat;

public final class SparkConfigurator {
    private SparkConfigurator() {
    }

    public static void configure(SparkSession spark, AppConfig config) {
        Mode mode = config.mode();
        if (mode == Mode.BENCHMARK || mode == Mode.INDEX_EXPERIMENT || mode == Mode.VALIDATE
                || config.outputFormats().contains(OutputFormat.ORC)) {
            OrcWriter.configureSpark(spark, config.orcWrite());
        }
        if (mode == Mode.BENCHMARK || mode == Mode.INDEX_EXPERIMENT || mode == Mode.VALIDATE
                || config.outputFormats().contains(OutputFormat.CARBON)) {
            CarbonWriter.configureSpark(spark);
        }
    }
}
