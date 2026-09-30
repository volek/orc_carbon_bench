package ru.sber.orcbench.benchmark;

import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Filter values sampled from an AUDEI-shaped audit row, plus AUDEI/ST windows and LIKE tokens.
 */
public final class FilterContext implements Serializable {
    private static final long serialVersionUID = 4L;

    public static final int EPK_PAGE_LIMIT = 1000;
    public static final int EPK_PAGE_LIMIT_MAX = 2000;

    private final String epkId;
    private final String eventId;
    private final String name;
    private final String channelType;
    private final String state;
    private final String module;
    private final String searchToken;
    private final Instant timestampStart;
    private final Instant timestampEnd;
    private final Instant window1dStart;
    private final Instant window1dEnd;
    private final Instant window14dStart;
    private final Instant window14dEnd;
    private final Instant window31dStart;
    private final Instant window31dEnd;
    private final int eventYear;
    private final int eventMonth;
    private final int eventDay;
    private final List<String> eventIdInList;
    private final List<String> epkIdInList;
    private final List<String> likeTokens;
    private final String rlikePattern;

    public FilterContext(
            String epkId,
            String eventId,
            String name,
            String channelType,
            String state,
            String module,
            String searchToken,
            Instant timestampStart,
            Instant timestampEnd,
            Instant window1dStart,
            Instant window1dEnd,
            Instant window14dStart,
            Instant window14dEnd,
            Instant window31dStart,
            Instant window31dEnd,
            int eventYear,
            int eventMonth,
            int eventDay,
            List<String> eventIdInList,
            List<String> epkIdInList,
            List<String> likeTokens,
            String rlikePattern
    ) {
        this.epkId = epkId;
        this.eventId = eventId;
        this.name = name;
        this.channelType = channelType;
        this.state = state;
        this.module = module;
        this.searchToken = searchToken;
        this.timestampStart = timestampStart;
        this.timestampEnd = timestampEnd;
        this.window1dStart = window1dStart;
        this.window1dEnd = window1dEnd;
        this.window14dStart = window14dStart;
        this.window14dEnd = window14dEnd;
        this.window31dStart = window31dStart;
        this.window31dEnd = window31dEnd;
        this.eventYear = eventYear;
        this.eventMonth = eventMonth;
        this.eventDay = eventDay;
        this.eventIdInList = eventIdInList;
        this.epkIdInList = epkIdInList;
        this.likeTokens = likeTokens;
        this.rlikePattern = rlikePattern;
    }

    public static FilterContext fromSample(
            Row row,
            long dataTimestampStartMs,
            long dataTimestampEndMs,
            long seed,
            int timestampWindowDays
    ) {
        String payload = row.getAs("payload_json");
        String token = "audit-event";
        if (payload != null && payload.contains("\"message\"")) {
            token = "audit-event";
        } else if (payload != null && payload.length() > 12) {
            token = payload.substring(Math.max(0, payload.length() / 2), Math.min(payload.length(), payload.length() / 2 + 8));
        }

        Instant[] defaultWindow = TimestampWindow.selective(
                dataTimestampStartMs, dataTimestampEndMs, seed, timestampWindowDays
        );
        Instant[] w1 = TimestampWindow.selective(dataTimestampStartMs, dataTimestampEndMs, seed + 11, 1);
        Instant[] w14 = TimestampWindow.selective(dataTimestampStartMs, dataTimestampEndMs, seed + 14, 14);
        Instant[] w31 = TimestampWindow.selective(dataTimestampStartMs, dataTimestampEndMs, seed + 31, 31);

        String eventId = row.getAs("event_id");
        String epkId = row.getAs("epk_id");
        String name = row.getAs("name");
        String channel = row.getAs("channel_type");
        int year = row.getInt(row.fieldIndex("event_year"));
        int month = row.getInt(row.fieldIndex("event_month"));
        int day = row.getInt(row.fieldIndex("event_day"));

        List<String> eventIds = Collections.unmodifiableList(Arrays.asList(
                eventId,
                eventId + "-missing-a",
                eventId + "-missing-b",
                eventId + "-missing-c"
        ));
        String prefix = epkId.length() >= 8 ? epkId.substring(0, 8) : epkId;
        List<String> epkIds = Collections.unmodifiableList(Arrays.asList(
                epkId,
                prefix + "-0000-4000-8000-000000000001",
                prefix + "-0000-4000-8000-000000000002",
                prefix + "-0000-4000-8000-000000000003"
        ));

        // ≥10 tokens for LIKE_FULLTEXT; first used for LIKE_SINGLE; 2–9 for LIKE_MULTI.
        List<String> likes = Collections.unmodifiableList(Arrays.asList(
                token,
                name == null ? "LOGON" : name,
                channel == null ? "WEB" : channel.substring(0, Math.min(3, channel.length())),
                "AUDEI",
                "sms",
                "param",
                "session",
                "device",
                "confirm",
                "metamodel",
                "pad",
                "event"
        ));

        String rlike = "(LOGON|FIND|ESA|LAUNCHER)";

        return new FilterContext(
                epkId,
                eventId,
                name,
                channel,
                row.getAs("state"),
                row.getAs("module"),
                token,
                defaultWindow[0],
                defaultWindow[1],
                w1[0],
                w1[1],
                w14[0],
                w14[1],
                w31[0],
                w31[1],
                year,
                month,
                day,
                eventIds,
                epkIds,
                likes,
                rlike
        );
    }

    public String epkId() {
        return epkId;
    }

    public String eventId() {
        return eventId;
    }

    public String name() {
        return name;
    }

    public String channelType() {
        return channelType;
    }

    public String state() {
        return state;
    }

    public String module() {
        return module;
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

    public Instant window1dStart() {
        return window1dStart;
    }

    public Instant window1dEnd() {
        return window1dEnd;
    }

    public Instant window14dStart() {
        return window14dStart;
    }

    public Instant window14dEnd() {
        return window14dEnd;
    }

    public Instant window31dStart() {
        return window31dStart;
    }

    public Instant window31dEnd() {
        return window31dEnd;
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

    public List<String> epkIdInList() {
        return epkIdInList;
    }

    public List<String> likeTokens() {
        return likeTokens;
    }

    public String rlikePattern() {
        return rlikePattern;
    }
}
