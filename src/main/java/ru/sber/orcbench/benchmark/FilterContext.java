package ru.sber.orcbench.benchmark;

import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class FilterContext implements Serializable {
    private static final long serialVersionUID = 2L;

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
    private final int eventYear;
    private final int eventMonth;
    private final int eventDay;
    private final List<String> eventIdInList;
    private final List<Long> productIdInList;

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
            Instant timestampEnd,
            int eventYear,
            int eventMonth,
            int eventDay,
            List<String> eventIdInList,
            List<Long> productIdInList
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
        this.eventYear = eventYear;
        this.eventMonth = eventMonth;
        this.eventDay = eventDay;
        this.eventIdInList = eventIdInList;
        this.productIdInList = productIdInList;
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

        String eventId = row.getAs("event_id");
        long productId = row.getLong(row.fieldIndex("product_id"));
        int year = row.getInt(row.fieldIndex("event_year"));
        int month = row.getInt(row.fieldIndex("event_month"));
        int day = row.getInt(row.fieldIndex("event_day"));

        List<String> eventIds = Collections.unmodifiableList(Arrays.asList(
                eventId,
                eventId + "-missing-a",
                eventId + "-missing-b",
                eventId + "-missing-c"
        ));
        List<Long> productIds = Collections.unmodifiableList(Arrays.asList(
                productId,
                productId + 1L,
                productId + 2L,
                productId + 3L
        ));

        return new FilterContext(
                eventId,
                row.getLong(row.fieldIndex("user_id")),
                row.getAs("country_code"),
                row.getAs("status"),
                productId,
                row.getLong(row.fieldIndex("campaign_id")),
                row.getAs("log_format"),
                token,
                window[0],
                window[1],
                year,
                month,
                day,
                eventIds,
                productIds
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

    public int eventYear() {
        return eventYear;
    }

    public int eventMonth() {
        return eventMonth;
    }

    public int eventDay() {
        return eventDay;
    }

    public List<String> eventIdInList() {
        return eventIdInList;
    }

    public List<Long> productIdInList() {
        return productIdInList;
    }
}
