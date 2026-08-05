package ru.sber.orcbench.config;

public final class OrcWriteSettings {
    public static final String[] DEFAULT_PARTITION_BY =
            {"event_year", "event_month", "event_day", "log_format"};

    private final String compression;
    private final int stripeSizeMb;
    private final int rowGroupSizeMb;
    private final int writePartitions;
    private final String[] partitionBy;

    public OrcWriteSettings(
            String compression,
            int stripeSizeMb,
            int rowGroupSizeMb,
            int writePartitions,
            String[] partitionBy
    ) {
        this.compression = compression;
        this.stripeSizeMb = stripeSizeMb;
        this.rowGroupSizeMb = rowGroupSizeMb;
        this.writePartitions = writePartitions;
        this.partitionBy = partitionBy;
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

    public boolean hasExplicitWritePartitions() {
        return writePartitions > 0;
    }
}
