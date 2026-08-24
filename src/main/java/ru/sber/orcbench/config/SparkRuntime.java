package ru.sber.orcbench.config;

public final class SparkRuntime {
    public static final String SPARK31_CARBON = "spark31-carbon";
    public static final String SPARK32_ORC = "spark32-orc";
    public static final String SPARK31_ARTIFACT = "orc-carbon-bench-spark31-all.jar";
    public static final String SPARK32_ARTIFACT = "orc-carbon-bench-spark32-all.jar";

    private SparkRuntime() {
    }

    public static boolean carbonAvailable() {
        try {
            Class.forName("org.apache.spark.sql.CarbonExtensions");
            return true;
        } catch (ClassNotFoundException ex) {
            return false;
        }
    }

    public static String runtimeId() {
        return carbonAvailable() ? SPARK31_CARBON : SPARK32_ORC;
    }

    public static void requireCompatible(AppConfig config) {
        if (carbonAvailable() || !requiresCarbon(config)) {
            return;
        }
        throw new IllegalStateException(
                "CarbonData and index experiments require " + SPARK31_ARTIFACT
                        + " submitted with BYOS Spark 3.1.1. "
                        + SPARK32_ARTIFACT + " supports ORC-only modes: "
                        + "generate --output-formats=orc, benchmark --formats=orc, report."
        );
    }

    public static boolean requiresCarbon(AppConfig config) {
        Mode mode = config.mode();
        if (mode == Mode.VALIDATE || mode == Mode.INDEX_EXPERIMENT) {
            return true;
        }
        if (mode == Mode.GENERATE) {
            return config.outputFormats().contains(OutputFormat.CARBON);
        }
        if (mode == Mode.BENCHMARK) {
            return config.benchmark().formats().contains(OutputFormat.CARBON);
        }
        return false;
    }

    public static String benchmarkOutputPath(AppConfig config) {
        return carbonAvailable() ? config.reportsRawPath() : config.reportsSpark32OrcPath();
    }
}
