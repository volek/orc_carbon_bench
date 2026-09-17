package ru.sber.orcbench.config;

import java.util.Locale;

/**
 * Cold vs warm cache protocol label for factor experiments.
 * Cold and warm results must never be averaged together.
 */
public enum CacheState {
    COLD("cold"),
    WARM("warm");

    private final String cliValue;

    CacheState(String cliValue) {
        this.cliValue = cliValue;
    }

    public String cliValue() {
        return cliValue;
    }

    public static CacheState fromCli(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return COLD;
        }
        String normalized = raw.trim().toLowerCase(Locale.ROOT);
        for (CacheState state : values()) {
            if (state.cliValue.equals(normalized)) {
                return state;
            }
        }
        throw new IllegalArgumentException(
                "Invalid --cache-state: " + raw + ". Allowed: cold, warm"
        );
    }
}
