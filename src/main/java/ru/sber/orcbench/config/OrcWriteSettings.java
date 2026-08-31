package ru.sber.orcbench.config;

import java.util.Arrays;
import java.util.Locale;

public final class OrcWriteSettings {
    public static final String[] DEFAULT_PARTITION_BY =
            {"event_year", "event_month", "event_day", "log_format"};

    public static final String[] DEFAULT_BLOOM_FILTER_COLUMNS =
            {"event_id", "user_id", "product_id", "campaign_id"};

    private static final double DEFAULT_BLOOM_FILTER_FPP = 0.05d;

    private final String compression;
    private final int stripeSizeMb;
    private final int rowGroupSizeMb;
    private final int writePartitions;
    private final String[] partitionBy;
    private final String[] bloomFilterColumns;
    private final double bloomFilterFpp;

    public OrcWriteSettings(
            String compression,
            int stripeSizeMb,
            int rowGroupSizeMb,
            int writePartitions,
            String[] partitionBy,
            String[] bloomFilterColumns,
            double bloomFilterFpp
    ) {
        this.compression = compression;
        this.stripeSizeMb = stripeSizeMb;
        this.rowGroupSizeMb = rowGroupSizeMb;
        this.writePartitions = writePartitions;
        this.partitionBy = partitionBy;
        this.bloomFilterColumns = bloomFilterColumns;
        this.bloomFilterFpp = bloomFilterFpp;
    }

    public String compression() {
        return compression;
    }

    public int stripeSizeMb() {
        return stripeSizeMb;
    }

    public int rowGroupSizeMb() {
        return rowGroupSizeMb;
    }

    public int writePartitions() {
        return writePartitions;
    }

    public String[] partitionBy() {
        return partitionBy;
    }

    public String[] bloomFilterColumns() {
        return bloomFilterColumns;
    }

    public double bloomFilterFpp() {
        return bloomFilterFpp;
    }

    public boolean bloomFiltersEnabled() {
        return bloomFilterColumns.length > 0;
    }

    public String bloomFilterColumnsCsv() {
        return String.join(",", bloomFilterColumns);
    }

    public boolean hasExplicitWritePartitions() {
        return writePartitions > 0;
    }

    /**
     * Parses {@code none} / empty as disabled; otherwise CSV column names.
     */
    public static String[] parseBloomFilterColumns(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return DEFAULT_BLOOM_FILTER_COLUMNS.clone();
        }
        String normalized = raw.trim();
        if ("none".equalsIgnoreCase(normalized)) {
            return new String[0];
        }
        String[] columns = ArgParser.parseCsv(normalized);
        if (columns.length == 0) {
            throw new IllegalArgumentException(
                    "Invalid argument for --orc-bloom-filter-columns: empty list (use 'none' to disable)"
            );
        }
        return columns;
    }

    public static double parseBloomFilterFpp(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return DEFAULT_BLOOM_FILTER_FPP;
        }
        try {
            double value = Double.parseDouble(raw.trim());
            if (Double.isNaN(value) || value <= 0.0d || value >= 1.0d) {
                throw new IllegalArgumentException(
                        "Argument --orc-bloom-filter-fpp must be in (0, 1): " + raw
                );
            }
            return value;
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid numeric argument for --orc-bloom-filter-fpp: " + raw, ex);
        }
    }

    @Override
    public String toString() {
        return "OrcWriteSettings{compression="
                + compression
                + ", bloom="
                + (bloomFiltersEnabled() ? bloomFilterColumnsCsv() : "none")
                + ", fpp="
                + bloomFilterFpp
                + "}";
    }
}
