package ru.sber.orcbench.config;

public record OrcWriteSettings(
        String compression,
        int stripeSizeMb,
        int rowGroupSizeMb,
        int writePartitions,
        String[] partitionBy
) {
    public static final String[] DEFAULT_PARTITION_BY =
            {"event_year", "event_month", "event_day", "log_format"};

    public boolean hasExplicitWritePartitions() {
        return writePartitions > 0;
    }
}
