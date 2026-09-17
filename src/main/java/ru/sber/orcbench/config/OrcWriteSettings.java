package ru.sber.orcbench.config;

public final class OrcWriteSettings {
    public static final String[] DEFAULT_PARTITION_BY =
            {"event_year", "event_month", "event_day", "log_format"};

    public static final String[] DEFAULT_BLOOM_FILTER_COLUMNS =
            {"event_id", "user_id", "product_id", "campaign_id"};

    public static final String[] BLOOM_HIGH_COLUMNS = {"event_id", "user_id"};
    public static final String[] BLOOM_MEDIUM_COLUMNS = {"product_id", "campaign_id"};

    private static final double DEFAULT_BLOOM_FILTER_FPP = 0.05d;
    public static final int DEFAULT_ROW_INDEX_STRIDE = 10_000;

    private final String compression;
    private final int stripeSizeMb;
    private final int rowGroupSizeMb;
    private final int rowIndexStride;
    private final int writePartitions;
    private final String[] partitionBy;
    private final String[] bloomFilterColumns;
    private final double bloomFilterFpp;
    private final String[] sortColumns;

    public OrcWriteSettings(
            String compression,
            int stripeSizeMb,
            int rowGroupSizeMb,
            int writePartitions,
            String[] partitionBy,
            String[] bloomFilterColumns,
            double bloomFilterFpp
    ) {
        this(
                compression,
                stripeSizeMb,
                rowGroupSizeMb,
                DEFAULT_ROW_INDEX_STRIDE,
                writePartitions,
                partitionBy,
                bloomFilterColumns,
                bloomFilterFpp,
                new String[0]
        );
    }

    public OrcWriteSettings(
            String compression,
            int stripeSizeMb,
            int rowGroupSizeMb,
            int rowIndexStride,
            int writePartitions,
            String[] partitionBy,
            String[] bloomFilterColumns,
            double bloomFilterFpp,
            String[] sortColumns
    ) {
        this.compression = compression;
        this.stripeSizeMb = stripeSizeMb;
        this.rowGroupSizeMb = rowGroupSizeMb;
        this.rowIndexStride = rowIndexStride;
        this.writePartitions = writePartitions;
        this.partitionBy = partitionBy;
        this.bloomFilterColumns = bloomFilterColumns;
        this.bloomFilterFpp = bloomFilterFpp;
        this.sortColumns = sortColumns;
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

    public int rowIndexStride() {
        return rowIndexStride;
    }

    public int writePartitions() {
        return writePartitions;
    }

    public String[] partitionBy() {
        return partitionBy;
    }

    public boolean partitioned() {
        return partitionBy.length > 0;
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

    public String[] sortColumns() {
        return sortColumns;
    }

    public boolean sorted() {
        return sortColumns.length > 0;
    }

    public String sortColumnsCsv() {
        return sorted() ? String.join(",", sortColumns) : "none";
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

    /**
     * Parses partition columns; {@code none} disables Hive-style partitioning.
     */
    public static String[] parsePartitionBy(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return DEFAULT_PARTITION_BY.clone();
        }
        if ("none".equalsIgnoreCase(raw.trim())) {
            return new String[0];
        }
        return ArgParser.parseCsv(raw);
    }

    /**
     * Parses sort columns; {@code none} / empty means unsorted write.
     */
    public static String[] parseSortColumns(String raw) {
        if (raw == null || raw.trim().isEmpty() || "none".equalsIgnoreCase(raw.trim())) {
            return new String[0];
        }
        return ArgParser.parseCsv(raw);
    }

    public static int parseRowIndexStride(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return DEFAULT_ROW_INDEX_STRIDE;
        }
        return ArgParser.parsePositiveInt(raw, "orc-row-index-stride");
    }

    @Override
    public String toString() {
        return "OrcWriteSettings{compression="
                + compression
                + ", stride="
                + rowIndexStride
                + ", bloom="
                + (bloomFiltersEnabled() ? bloomFilterColumnsCsv() : "none")
                + ", fpp="
                + bloomFilterFpp
                + ", sort="
                + sortColumnsCsv()
                + ", partitionBy="
                + (partitioned() ? String.join(",", partitionBy) : "none")
                + "}";
    }
}
