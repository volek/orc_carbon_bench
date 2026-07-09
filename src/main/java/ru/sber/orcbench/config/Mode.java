package ru.sber.orcbench.config;

public enum Mode {
    GENERATE("generate"),
    VALIDATE("validate"),
    INDEX_EXPERIMENT("index-experiment"),
    BENCHMARK("benchmark"),
    REPORT("report");

    private final String cliValue;

    Mode(String cliValue) {
        this.cliValue = cliValue;
    }

    public static Mode fromCli(String value) {
        for (Mode mode : values()) {
            if (mode.cliValue.equalsIgnoreCase(value)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("Unknown mode: " + value);
    }

    public String cliValue() {
        return cliValue;
    }
}
