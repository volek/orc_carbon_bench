package ru.sber.orcbench.config;

public record CarbonWriteSettings(
        String tableName,
        String compression,
        boolean enableBloomIndex,
        String[] bloomIndexColumns,
        boolean enableLuceneIndex,
        String[] luceneIndexColumns,
        int writePartitions,
        String[] partitionBy
) {
    public static final String[] DEFAULT_BLOOM_COLUMNS = {"user_id", "product_id"};
    public static final String[] DEFAULT_LUCENE_COLUMNS = {"log_message"};
    public static final String[] DEFAULT_PARTITION_BY =
            {"event_year", "event_month", "event_day", "log_format"};

    public boolean hasExplicitWritePartitions() {
        return writePartitions > 0;
    }
}
