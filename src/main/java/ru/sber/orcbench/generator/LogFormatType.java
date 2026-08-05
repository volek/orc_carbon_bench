package ru.sber.orcbench.generator;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public enum LogFormatType {
    JSON("json"),
    PLAIN_TEXT("plain_text"),
    KEY_VALUE("key_value"),
    APACHE_COMMON("apache_common");

    public static final List<String> ALL_VALUES = Arrays.stream(values())
            .map(LogFormatType::value)
            .collect(Collectors.toList());

    private final String value;

    LogFormatType(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    public static LogFormatType fromIndex(long index) {
        LogFormatType[] values = values();
        return values[(int) Math.floorMod(index, values.length)];
    }
}
