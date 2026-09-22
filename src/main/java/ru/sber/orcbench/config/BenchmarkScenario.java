package ru.sber.orcbench.config;

import java.util.EnumSet;
import java.util.Set;

/**
 * Benchmark query scenarios: legacy doc Q1–Q10, AUDEI interactive, ST archive mix.
 */
public enum BenchmarkScenario {
    // --- legacy document suite (mapped onto AUDEI columns) ---
    FULL_SCAN("full_scan"),
    PROJECTION("projection"),
    PARTITION_PRUNE("partition_prune"),
    FILTER_LOW_CARDINALITY("filter_low_cardinality"),
    FILTER_MEDIUM_CARDINALITY("filter_medium_cardinality"),
    FILTER_HIGH_CARDINALITY("filter_high_cardinality"),
    FILTER_IN("filter_in"),
    FILTER_TIMESTAMP_RANGE("filter_timestamp_range"),
    FILTER_LOG_FORMAT("filter_log_format"),
    FILTER_COMBINED("filter_combined"),
    GROUP_BY("group_by"),
    GROUP_BY_HEAVY("group_by_heavy"),
    JOIN_DICTIONARY("join_dictionary"),
    TEXT_SEARCH("text_search"),

    // --- AUDEI interactive (SLA ≤ 3s path) ---
    EPK_EQ_1D("epk_eq_1d"),
    EPK_EQ_14D("epk_eq_14d"),
    EPK_PAGE("epk_page"),
    EQ_FILTERS("eq_filters"),
    ORDER_BY_EPK_DAY("order_by_epk_day"),

    // --- ST archive / operator mix ---
    NO_FILTER("no_filter"),
    LIKE_SINGLE("like_single"),
    LIKE_MULTI("like_multi"),
    LIKE_FULLTEXT("like_fulltext"),
    EQ("eq"),
    IN_LIST("in_list"),
    RLIKE("rlike");

    private final String cliValue;

    BenchmarkScenario(String cliValue) {
        this.cliValue = cliValue;
    }

    public String cliValue() {
        return cliValue;
    }

    /** ST research query_category label (or {@code null} for non-ST scenarios). */
    public String queryCategory() {
        switch (this) {
            case NO_FILTER:
                return "8_NO_FILTER";
            case LIKE_FULLTEXT:
                return "3_LIKE_FULLTEXT";
            case LIKE_MULTI:
                return "4_LIKE_MULTI";
            case LIKE_SINGLE:
                return "5_LIKE_SINGLE";
            case IN_LIST:
                return "6_IN";
            case EQ:
            case EQ_FILTERS:
            case EPK_EQ_1D:
            case EPK_EQ_14D:
            case EPK_PAGE:
                return "7_EQ";
            case RLIKE:
                return "2_RLIKE";
            default:
                return null;
        }
    }

    /** {@code interactive} (AUDEI API SLA) vs {@code archive} (ST Abyss-style scan). */
    public String slaClass() {
        switch (this) {
            case EPK_EQ_1D:
            case EPK_EQ_14D:
            case EPK_PAGE:
            case EQ_FILTERS:
                return "interactive";
            case ORDER_BY_EPK_DAY:
            case NO_FILTER:
            case LIKE_SINGLE:
            case LIKE_MULTI:
            case LIKE_FULLTEXT:
            case EQ:
            case IN_LIST:
            case RLIKE:
                return "archive";
            default:
                return "legacy";
        }
    }

    /** Approximate search window days for ST duration_group labeling. */
    public int searchWindowDays() {
        switch (this) {
            case EPK_EQ_1D:
            case EQ_FILTERS:
            case ORDER_BY_EPK_DAY:
            case PARTITION_PRUNE:
                return 1;
            case EPK_EQ_14D:
            case EPK_PAGE:
                return 14;
            case NO_FILTER:
            case LIKE_SINGLE:
            case LIKE_MULTI:
            case LIKE_FULLTEXT:
            case EQ:
            case IN_LIST:
            case RLIKE:
                return 31;
            case FILTER_TIMESTAMP_RANGE:
            case FILTER_COMBINED:
                return 30;
            default:
                return 30;
        }
    }

    public String durationGroup() {
        return ExperimentMeta.durationGroup(searchWindowDays());
    }

    public static BenchmarkScenario fromCli(String value) {
        String normalized = value.trim().toLowerCase(java.util.Locale.ROOT);
        for (BenchmarkScenario scenario : values()) {
            if (scenario.cliValue.equals(normalized)) {
                return scenario;
            }
        }
        throw new IllegalArgumentException("Unknown benchmark scenario: " + value);
    }

    public static Set<BenchmarkScenario> parseCsv(String raw) {
        String trimmed = raw.trim();
        if ("all".equalsIgnoreCase(trimmed)) {
            return EnumSet.allOf(BenchmarkScenario.class);
        }
        if ("doc".equalsIgnoreCase(trimmed) || "q1-q10".equalsIgnoreCase(trimmed)) {
            return documentSuite();
        }
        if ("audei".equalsIgnoreCase(trimmed)) {
            return audeiSuite();
        }
        if ("st".equalsIgnoreCase(trimmed)) {
            return stSuite();
        }
        EnumSet<BenchmarkScenario> scenarios = EnumSet.noneOf(BenchmarkScenario.class);
        for (String part : ArgParser.parseCsv(raw)) {
            scenarios.add(fromCli(part));
        }
        if (scenarios.isEmpty()) {
            throw new IllegalArgumentException("Invalid argument for --benchmark-scenarios: empty list");
        }
        return scenarios;
    }

    /** Document Q1–Q10 core suite (excluding auxiliary log_format/text_search). */
    public static Set<BenchmarkScenario> documentSuite() {
        return EnumSet.of(
                PARTITION_PRUNE,
                FILTER_HIGH_CARDINALITY,
                FILTER_MEDIUM_CARDINALITY,
                FILTER_LOW_CARDINALITY,
                FILTER_IN,
                FILTER_TIMESTAMP_RANGE,
                PROJECTION,
                FULL_SCAN,
                GROUP_BY,
                GROUP_BY_HEAVY,
                JOIN_DICTIONARY
        );
    }

    /** AUDEI interactive access patterns (epk_id + date, filters, paging). */
    public static Set<BenchmarkScenario> audeiSuite() {
        return EnumSet.of(
                EPK_EQ_1D,
                EPK_EQ_14D,
                EPK_PAGE,
                EQ_FILTERS,
                ORDER_BY_EPK_DAY
        );
    }

    /** ST stand archive operator mix (NO_FILTER / LIKE / EQ / IN / RLIKE). */
    public static Set<BenchmarkScenario> stSuite() {
        return EnumSet.of(
                NO_FILTER,
                LIKE_SINGLE,
                LIKE_MULTI,
                LIKE_FULLTEXT,
                EQ,
                IN_LIST,
                RLIKE
        );
    }
}
