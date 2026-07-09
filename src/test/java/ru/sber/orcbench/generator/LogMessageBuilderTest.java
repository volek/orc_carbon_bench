package ru.sber.orcbench.generator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class LogMessageBuilderTest {

    @Test
    void buildsJsonFormat() {
        String message = LogMessageBuilder.build(
                "json", "evt-1", 100L, "sess-1", "RU", "mobile", "success",
                10L, 20L, 1, 1_700_000_000_000L, 42.5
        );
        assertTrue(message.startsWith("{"));
        assertTrue(message.contains("\"event_id\":\"evt-1\""));
        assertTrue(message.contains("\"country_code\":\"RU\""));
    }

    @Test
    void buildsKeyValueFormat() {
        String message = LogMessageBuilder.build(
                "key_value", "evt-2", 200L, "sess-2", "US", "desktop", "failed",
                11L, 21L, 2, 1_700_000_000_000L, 10.0
        );
        assertTrue(message.contains("event_id=evt-2"));
        assertTrue(message.contains("device_type=desktop"));
    }

    @Test
    void buildsApacheCommonFormat() {
        String message = LogMessageBuilder.build(
                "apache_common", "evt-3", 3L, "sess-3", "DE", "tablet", "pending",
                12L, 22L, 3, 1_700_000_000_000L, 99.0
        );
        assertTrue(message.contains("HTTP/1.1"));
        assertTrue(message.contains("/events/evt-3"));
    }
}
