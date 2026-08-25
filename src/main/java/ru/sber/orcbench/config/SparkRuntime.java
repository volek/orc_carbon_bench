package ru.sber.orcbench.config;

public final class SparkRuntime {
    public static final String SPARK32_ORC = "spark32-orc";
    public static final String ARTIFACT = "orc-bench-all.jar";

    private SparkRuntime() {
    }

    public static String runtimeId() {
        return SPARK32_ORC;
    }
}
