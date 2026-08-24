package ru.sber.orcbench.config;

import org.apache.spark.sql.SparkSession;

public final class SparkRuntimeInfo {
    private final String sparkVersion;
    private final String sparkRuntime;

    public SparkRuntimeInfo(String sparkVersion, String sparkRuntime) {
        this.sparkVersion = sparkVersion;
        this.sparkRuntime = sparkRuntime;
    }

    public static SparkRuntimeInfo from(SparkSession spark) {
        return new SparkRuntimeInfo(spark.version(), SparkRuntime.runtimeId());
    }

    public String sparkVersion() {
        return sparkVersion;
    }

    public String sparkRuntime() {
        return sparkRuntime;
    }
}
