package ru.sber.orcbench.config;

import java.util.Locale;

/**
 * Query engine under test. Spark path is in-process; Hive is script/Beeline driven.
 */
public enum EngineType {
    SPARK("spark"),
    HIVE_TEZ("hive_tez"),
    HIVE_LLAP("hive_llap");

    private final String cliValue;

    EngineType(String cliValue) {
        this.cliValue = cliValue;
    }

    public String cliValue() {
        return cliValue;
    }

    public static EngineType fromCli(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return SPARK;
        }
        String normalized = raw.trim().toLowerCase(Locale.ROOT);
        for (EngineType engine : values()) {
            if (engine.cliValue.equals(normalized)) {
                return engine;
            }
        }
        throw new IllegalArgumentException(
                "Invalid --engine: " + raw + ". Allowed: spark, hive_tez, hive_llap"
        );
    }
}
