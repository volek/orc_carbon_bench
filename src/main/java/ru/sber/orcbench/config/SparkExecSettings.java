package ru.sber.orcbench.config;

import java.util.Map;

/**
 * Spark SQL execution toggles for factor experiments L–O (vectorized, pushdown, AQE, DPP, CBO).
 */
public final class SparkExecSettings {
    private final boolean filterPushdown;
    private final boolean vectorizedReader;
    private final boolean adaptiveExecution;
    private final boolean dynamicPartitionPruning;
    private final boolean costBasedOptimization;

    public SparkExecSettings(
            boolean filterPushdown,
            boolean vectorizedReader,
            boolean adaptiveExecution,
            boolean dynamicPartitionPruning,
            boolean costBasedOptimization
    ) {
        this.filterPushdown = filterPushdown;
        this.vectorizedReader = vectorizedReader;
        this.adaptiveExecution = adaptiveExecution;
        this.dynamicPartitionPruning = dynamicPartitionPruning;
        this.costBasedOptimization = costBasedOptimization;
    }

    public static SparkExecSettings defaults() {
        return new SparkExecSettings(true, true, true, true, true);
    }

    public static SparkExecSettings from(Map<String, String> kv) {
        return new SparkExecSettings(
                ArgParser.parseBoolean(kv.getOrDefault("spark-orc-filter-pushdown", "true"), "spark-orc-filter-pushdown"),
                ArgParser.parseBoolean(kv.getOrDefault("spark-orc-vectorized", "true"), "spark-orc-vectorized"),
                ArgParser.parseBoolean(kv.getOrDefault("spark-aqe", "true"), "spark-aqe"),
                ArgParser.parseBoolean(kv.getOrDefault("spark-dpp", "true"), "spark-dpp"),
                ArgParser.parseBoolean(kv.getOrDefault("spark-cbo", "true"), "spark-cbo")
        );
    }

    public boolean filterPushdown() {
        return filterPushdown;
    }

    public boolean vectorizedReader() {
        return vectorizedReader;
    }

    public boolean adaptiveExecution() {
        return adaptiveExecution;
    }

    public boolean dynamicPartitionPruning() {
        return dynamicPartitionPruning;
    }

    public boolean costBasedOptimization() {
        return costBasedOptimization;
    }

    public String profileLabel() {
        return "pushdown=" + filterPushdown
                + ",vec=" + vectorizedReader
                + ",aqe=" + adaptiveExecution
                + ",dpp=" + dynamicPartitionPruning
                + ",cbo=" + costBasedOptimization;
    }
}
