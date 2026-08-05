package ru.sber.orcbench.config;

public final class CarbonWriteSettings {
    public static final String[] DEFAULT_BLOOM_COLUMNS = {"user_id", "product_id"};
    public static final String[] DEFAULT_LUCENE_COLUMNS = {"log_message"};
    public static final String[] DEFAULT_PARTITION_BY =
            {"event_year", "event_month", "event_day", "log_format"};

    private final String tableName;
    private final String compression;
    private final boolean enableBloomIndex;
    private final String[] bloomIndexColumns;
    private final boolean enableLuceneIndex;
    private final String[] luceneIndexColumns;
    private final int writePartitions;
    private final String[] partitionBy;

    public CarbonWriteSettings(
            String tableName,
            String compression,
            boolean enableBloomIndex,
            String[] bloomIndexColumns,
            boolean enableLuceneIndex,
            String[] luceneIndexColumns,
            int writePartitions,
            String[] partitionBy
    ) {
        this.tableName = tableName;
        this.compression = compression;
        this.enableBloomIndex = enableBloomIndex;
        this.bloomIndexColumns = bloomIndexColumns;
        this.enableLuceneIndex = enableLuceneIndex;
        this.luceneIndexColumns = luceneIndexColumns;
        this.writePartitions = writePartitions;
        this.partitionBy = partitionBy;
    }

    public String tableName() {
        return tableName;
    }

    public String compression() {
        return compression;
    }

    public boolean enableBloomIndex() {
        return enableBloomIndex;
    }

    public String[] bloomIndexColumns() {
        return bloomIndexColumns;
    }

    public boolean enableLuceneIndex() {
        return enableLuceneIndex;
    }

    public String[] luceneIndexColumns() {
        return luceneIndexColumns;
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
