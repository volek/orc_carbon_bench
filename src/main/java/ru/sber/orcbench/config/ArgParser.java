package ru.sber.orcbench.config;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalInt;

public final class ArgParser {
    private ArgParser() {
    }

    public static String[] parseCsv(String raw) {
        return Arrays.stream(raw.split(","))
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .toArray(String[]::new);
    }

    public static boolean parseBoolean(String raw, String key) {
        String normalized = raw.trim().toLowerCase(Locale.ROOT);
        switch (normalized) {
            case "true":
            case "1":
            case "yes":
            case "y":
                return true;
            case "false":
            case "0":
            case "no":
            case "n":
                return false;
            default:
                throw new IllegalArgumentException("Invalid boolean argument for --" + key + ": " + raw);
        }
    }

    public static OptionalInt parseOptionalPositiveInt(Map<String, String> kv, String key) {
        if (!kv.containsKey(key)) {
            return OptionalInt.empty();
        }
        return OptionalInt.of((int) parsePositiveLong(kv.get(key), key));
    }

    public static long parsePositiveLong(String raw, String key) {
        try {
            long value = Long.parseLong(raw.trim());
            if (value <= 0) {
                throw new IllegalArgumentException("Argument --" + key + " must be positive: " + raw);
            }
            return value;
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid numeric argument for --" + key + ": " + raw, ex);
        }
    }

    public static int parseNonNegativeInt(String raw, String key) {
        try {
            int value = Integer.parseInt(raw.trim());
            if (value < 0) {
                throw new IllegalArgumentException("Argument --" + key + " must be non-negative: " + raw);
            }
            return value;
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid numeric argument for --" + key + ": " + raw, ex);
        }
    }

    public static int parsePositiveInt(String raw, String key) {
        try {
            int value = Integer.parseInt(raw.trim());
            if (value <= 0) {
                throw new IllegalArgumentException("Argument --" + key + " must be positive: " + raw);
            }
            return value;
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid numeric argument for --" + key + ": " + raw, ex);
        }
    }

    public static String parseEnum(String raw, String key, String... allowed) {
        for (String value : allowed) {
            if (value.equalsIgnoreCase(raw.trim())) {
                return value.toLowerCase(Locale.ROOT);
            }
        }
        throw new IllegalArgumentException(
                "Invalid argument for --" + key + ": " + raw + ". Allowed: " + String.join(", ", allowed)
        );
    }
}
