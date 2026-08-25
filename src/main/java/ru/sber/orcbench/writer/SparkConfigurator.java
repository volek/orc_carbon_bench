package ru.sber.orcbench.writer;

import org.apache.spark.sql.SparkSession;
import ru.sber.orcbench.config.AppConfig;
import ru.sber.orcbench.config.Mode;

public final class SparkConfigurator {
    private SparkConfigurator() {
    }

    public static SparkSession.Builder configureBuilder(SparkSession.Builder builder, AppConfig config) {
        return builder;
    }

    public static void configure(SparkSession spark, AppConfig config) {
        if (modeNeedsOrc(config)) {
            OrcWriter.configureSpark(spark, config.orcWrite());
        }
    }

    private static boolean modeNeedsOrc(AppConfig config) {
        Mode mode = config.mode();
        return mode == Mode.BENCHMARK || mode == Mode.VALIDATE || mode == Mode.GENERATE;
    }
}
