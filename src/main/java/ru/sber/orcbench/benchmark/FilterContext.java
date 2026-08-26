package ru.sber.orcbench.benchmark;

import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.time.Instant;

public final class FilterContext implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String eventId;
    private final long userId;
    private final String countryCode;
    private final String status;
    private final long productId;
    private final long campaignId;
    private final String logFormat;
    private final String searchToken;
    private final Instant timestampStart;
    private final Instant timestampEnd;

    public FilterContext(
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
    ) {
        this.eventId = eventId;
        this.userId = userId;
        this.countryCode = countryCode;
        this.status = status;
        this.productId = productId;
        this.campaignId = campaignId;
        this.logFormat = logFormat;
        this.searchToken = searchToken;
        this.timestampStart = timestampStart;
        this.timestampEnd = timestampEnd;
    }

    /**
     * Builds filter values from a sample row and a selective timestamp window inside the generate span.
     */
    public static FilterContext fromSample(
            Row row,
            long dataTimestampStartMs,
            long dataTimestampEndMs,
            long seed,
            int timestampWindowDays
    ) {
        String logMessage = row.getAs("log_message");
        String token = "mobile";
        if (logMessage != null && logMessage.length() > 8) {
            token = logMessage.substring(0, Math.min(8, logMessage.length()));
        }

        Instant[] window = TimestampWindow.selective(
                dataTimestampStartMs,
                dataTimestampEndMs,
                seed,
                timestampWindowDays
        );

        return new FilterContext(
                row.getAs("event_id"),
                row.getLong(row.fieldIndex("user_id")),
                row.getAs("country_code"),
                row.getAs("status"),
                row.getLong(row.fieldIndex("product_id")),
                row.getLong(row.fieldIndex("campaign_id")),
                row.getAs("log_format"),
                token,
                window[0],
                window[1]
        );
    }

    public String eventId() {
        return eventId;
    }

    public long userId() {
        return userId;
    }

    public String countryCode() {
        return countryCode;
    }

    public String status() {
        return status;
    }

    public long productId() {
        return productId;
    }

    public long campaignId() {
        return campaignId;
    }

    public String logFormat() {
        return logFormat;
    }

    public String searchToken() {
        return searchToken;
    }

    public Instant timestampStart() {
        return timestampStart;
    }

    public Instant timestampEnd() {
        return timestampEnd;
    }
}
