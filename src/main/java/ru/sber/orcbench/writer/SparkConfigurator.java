package ru.sber.orcbench.writer;

import org.apache.spark.sql.SparkSession;
import ru.sber.orcbench.config.AppConfig;
import ru.sber.orcbench.config.Mode;
import ru.sber.orcbench.config.OutputFormat;

public final class SparkConfigurator {
    private static final String CARBON_EXTENSIONS = "org.apache.spark.sql.CarbonExtensions";
    private static final String CARBON_CATALOG = "org.apache.spark.sql.CarbonSessionCatalog";

    private SparkConfigurator() {
    }

    public static SparkSession.Builder configureBuilder(SparkSession.Builder builder, AppConfig config) {
        if (needsCarbon(config)) {
            builder.config("spark.sql.extensions", CARBON_EXTENSIONS)
                    .config("spark.sql.catalog.spark_catalog", CARBON_CATALOG);
        }
        return builder;
    }

    public static void configure(SparkSession spark, AppConfig config) {
        Mode mode = config.mode();
        if (mode == Mode.BENCHMARK || mode == Mode.INDEX_EXPERIMENT || mode == Mode.VALIDATE
                || config.outputFormats().contains(OutputFormat.ORC)) {
            OrcWriter.configureSpark(spark, config.orcWrite());
        }
    }

    private static boolean needsCarbon(AppConfig config) {
        Mode mode = config.mode();
        return mode == Mode.BENCHMARK || mode == Mode.INDEX_EXPERIMENT || mode == Mode.VALIDATE
                || config.outputFormats().contains(OutputFormat.CARBON);
    }
}
