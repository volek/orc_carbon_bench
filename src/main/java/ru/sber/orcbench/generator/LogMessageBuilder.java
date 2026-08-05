package ru.sber.orcbench.generator;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public final class LogMessageBuilder implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final DateTimeFormatter APACHE_TS =
            DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z").withZone(ZoneOffset.UTC);

    private LogMessageBuilder() {
    }

    public static String build(
            String logFormat,
            String eventId,
            long userId,
            String sessionId,
            String countryCode,
            String deviceType,
            String status,
            long productId,
            long campaignId,
            int regionId,
            long timestampMs,
            double amount
    ) {
        switch (logFormat) {
            case "json":
                return buildJson(
                        eventId, userId, sessionId, countryCode, deviceType, status,
                        productId, campaignId, regionId, timestampMs, amount
                );
            case "plain_text":
                return buildPlainText(
                        eventId, userId, sessionId, countryCode, deviceType, status,
                        productId, campaignId, regionId, timestampMs, amount
                );
            case "key_value":
                return buildKeyValue(
                        eventId, userId, sessionId, countryCode, deviceType, status,
                        productId, campaignId, regionId, timestampMs, amount
                );
            case "apache_common":
                return buildApacheCommon(
                        eventId, userId, sessionId, countryCode, deviceType, status,
                        productId, campaignId, regionId, timestampMs, amount
                );
            default:
                return buildJson(
                        eventId, userId, sessionId, countryCode, deviceType, status,
                        productId, campaignId, regionId, timestampMs, amount
                );
        }
    }

    private static String buildJson(
            String eventId, long userId, String sessionId, String countryCode, String deviceType,
            String status, long productId, long campaignId, int regionId, long timestampMs, double amount
    ) {
        return "{"
                + "\"event_id\":\"" + eventId + "\","
                + "\"user_id\":" + userId + ","
                + "\"session_id\":\"" + sessionId + "\","
                + "\"country_code\":\"" + countryCode + "\","
                + "\"device_type\":\"" + deviceType + "\","
                + "\"status\":\"" + status + "\","
                + "\"product_id\":" + productId + ","
                + "\"campaign_id\":" + campaignId + ","
                + "\"region_id\":" + regionId + ","
                + "\"timestamp\":" + timestampMs + ","
                + "\"amount\":" + amount
                + "}";
    }

    private static String buildPlainText(
            String eventId, long userId, String sessionId, String countryCode, String deviceType,
            String status, long productId, long campaignId, int regionId, long timestampMs, double amount
    ) {
        return "event " + eventId
                + " user=" + userId
                + " session=" + sessionId
                + " country=" + countryCode
                + " device=" + deviceType
                + " status=" + status
                + " product=" + productId
                + " campaign=" + campaignId
                + " region=" + regionId
                + " ts=" + timestampMs
                + " amount=" + amount;
    }

    private static String buildKeyValue(
            String eventId, long userId, String sessionId, String countryCode, String deviceType,
            String status, long productId, long campaignId, int regionId, long timestampMs, double amount
    ) {
        return "event_id=" + eventId
                + " user_id=" + userId
                + " session_id=" + sessionId
                + " country_code=" + countryCode
                + " device_type=" + deviceType
                + " status=" + status
                + " product_id=" + productId
                + " campaign_id=" + campaignId
                + " region_id=" + regionId
                + " timestamp=" + timestampMs
                + " amount=" + amount;
    }

    private static String buildApacheCommon(
            String eventId, long userId, String sessionId, String countryCode, String deviceType,
            String status, long productId, long campaignId, int regionId, long timestampMs, double amount
    ) {
        String ts = APACHE_TS.format(Instant.ofEpochMilli(timestampMs));
        return "127.0.0." + (Math.floorMod(userId, 254) + 1)
                + " - user" + userId + " [" + ts + "] \"GET /events/" + eventId
                + " HTTP/1.1\" 200 " + (int) amount
                + " \"-\" \"device=" + deviceType + ";country=" + countryCode
                + ";status=" + status + ";session=" + sessionId
                + ";product=" + productId + ";campaign=" + campaignId
                + ";region=" + regionId + "\"";
    }
}
