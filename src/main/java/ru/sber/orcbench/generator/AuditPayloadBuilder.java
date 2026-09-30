package ru.sber.orcbench.generator;

import java.io.Serializable;
import java.util.UUID;

/**
 * Builds AUDEI-like audit JSON payloads sized near {@code targetBytes}.
 */
public final class AuditPayloadBuilder implements Serializable {
    private static final long serialVersionUID = 1L;

    private AuditPayloadBuilder() {
    }

    public static String build(
            String epkId,
            String eventId,
            String session,
            String module,
            String name,
            String channelType,
            String state,
            String userLogin,
            long eventTsMs,
            int targetBytes
    ) {
        StringBuilder sb = new StringBuilder(Math.max(512, targetBytes));
        sb.append('{')
                .append("\"__time\":\"").append(eventTsMs).append("\",")
                .append("\"epk_id\":\"").append(epkId).append("\",")
                .append("\"id\":\"").append(eventId).append("\",")
                .append("\"session\":\"").append(session).append("\",")
                .append("\"module\":\"").append(module).append("\",")
                .append("\"name\":\"").append(name).append("\",")
                .append("\"channelType\":\"").append(channelType).append("\",")
                .append("\"state\":\"").append(state).append("\",")
                .append("\"userLogin\":\"").append(userLogin).append("\",")
                .append("\"metamodelVersion\":\"228\",")
                .append("\"paramName\":[")
                .append("\"type\",\"application\",\"channelType\",\"state\",\"message\",")
                .append("\"sessionId\",\"deviceID\",\"operationUID\",\"ipAddress\",\"confirmType\"")
                .append("],")
                .append("\"paramValue\":[")
                .append('"').append(name).append("\",")
                .append("\"AUDEI_BENCH\",")
                .append('"').append(channelType).append("\",")
                .append('"').append(state).append("\",")
                .append("\"audit-event\",")
                .append('"').append(session).append("\",")
                .append('"').append(UUID.nameUUIDFromBytes(eventId.getBytes()).toString()).append("\",")
                .append('"').append(eventId).append("\",")
                .append("\"127.0.0.1\",")
                .append("\"sms\"")
                .append(']');

        String closerPrefix = ",\"pad\":\"";
        String closerSuffix = "\"}";
        int current = sb.length() + closerPrefix.length() + closerSuffix.length();
        int padNeed = Math.max(0, targetBytes - current);
        sb.append(closerPrefix);
        for (int i = 0; i < padNeed; i++) {
            sb.append((char) ('a' + (i % 26)));
        }
        sb.append(closerSuffix);
        return sb.toString();
    }
}
