package ru.sber.orcbench.writer;

import org.apache.spark.sql.SparkSession;
import ru.sber.orcbench.config.AppConfig;
import ru.sber.orcbench.config.Mode;
import ru.sber.orcbench.config.OutputFormat;

public final class SparkConfigurator {
    private SparkConfigurator() {
    }

    public static SparkSession.Builder configureBuilder(SparkSession.Builder builder, AppConfig config) {
        if (needsCarbon(config)) {
            CarbonWriter.configureBuilder(builder);
        }
        return builder;
    }

    public static void configure(SparkSession spark, AppConfig config) {
        if (modeNeedsOrc(config)) {
            OrcWriter.configureSpark(spark, config.orcWrite());
        }
        if (needsCarbon(config)) {
            CarbonWriter.requireConfigured(spark);
        }
    }

    private static boolean modeNeedsOrc(AppConfig config) {
        Mode mode = config.mode();
        return mode == Mode.BENCHMARK || mode == Mode.INDEX_EXPERIMENT || mode == Mode.VALIDATE
                || config.outputFormats().contains(OutputFormat.ORC);
    }

    private static boolean needsCarbon(AppConfig config) {
        Mode mode = config.mode();
        return mode == Mode.BENCHMARK || mode == Mode.INDEX_EXPERIMENT || mode == Mode.VALIDATE
                || config.outputFormats().contains(OutputFormat.CARBON);
    }
}
