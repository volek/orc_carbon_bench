package ru.sber.orcbench.generator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class AuditPayloadBuilderTest {

    @Test
    void buildsJsonWithAudeiFields() {
        String payload = AuditPayloadBuilder.build(
                "003a75de-2a9b-45bc-89c9-1f9a8ebd9b0c",
                "evt-1",
                "sess-1",
                "CI02001608_sm_uko",
                "LOGON",
                "WEB_SBOL",
                "success",
                "user_1",
                1_700_000_000_000L,
                800
        );
        assertTrue(payload.startsWith("{"));
        assertTrue(payload.contains("\"epk_id\":\"003a75de-2a9b-45bc-89c9-1f9a8ebd9b0c\""));
        assertTrue(payload.contains("\"name\":\"LOGON\""));
        assertTrue(payload.contains("\"channelType\":\"WEB_SBOL\""));
        assertTrue(payload.length() >= 800);
    }

    @Test
    void padsToTargetBytes() {
        String payload = AuditPayloadBuilder.build(
                "epk", "evt", "sess", "mod", "ESA", "ATM", "failed", "u", 1L, 1600
        );
        assertTrue(payload.length() >= 1590 && payload.length() <= 1610);
    }
}
