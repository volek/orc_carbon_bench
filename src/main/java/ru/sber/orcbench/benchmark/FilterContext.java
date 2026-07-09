package ru.sber.orcbench.benchmark;

import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.time.Instant;

public record FilterContext(
        String eventId,
        long userId,
        String countryCode,
        String status,
        long productId,
        long campaignId,
        String logFormat,
        String searchToken,
        Instant timestampStart,
        Instant timestampEnd
) implements Serializable {
    public static FilterContext fromSample(Row row, long timestampStartMs, long timestampEndMs) {
        String logMessage = row.getAs("log_message");
        String token = "mobile";
        if (logMessage != null && logMessage.length() > 8) {
            token = logMessage.substring(0, Math.min(8, logMessage.length()));
        }

        return new FilterContext(
                row.getAs("event_id"),
                row.getLong(row.fieldIndex("user_id")),
                row.getAs("country_code"),
                row.getAs("status"),
                row.getLong(row.fieldIndex("product_id")),
                row.getLong(row.fieldIndex("campaign_id")),
                row.getAs("log_format"),
                token,
                Instant.ofEpochMilli(timestampStartMs),
                Instant.ofEpochMilli(timestampEndMs)
        );
    }
}
