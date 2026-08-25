package ru.sber.orcbench.generator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sber.orcbench.writer.OrcWriter;

public final class GenerateRunner {
    private static final Logger LOG = LoggerFactory.getLogger(GenerateRunner.class);

    private GenerateRunner() {
    }

    public static void run(SparkSession spark, GeneratorConfig config) {
        long totalRows = config.estimatedTotalRows();
        int chunkCount = config.chunkCount();
        long rowsPerChunk = config.rowsPerChunk();

        LOG.info(
                "Generator plan: orcPath={} targetSizeTb={} estimatedRows={} "
                        + "chunks={} rowsPerChunk={} timeRange=[{} .. {}] chunkDays={} avgRowBytes={}",
                config.orcPath(),
                config.targetSizeTb(),
                totalRows,
                chunkCount,
                rowsPerChunk,
                config.timestampStart(),
                config.timestampEnd(),
                config.chunkDays(),
                config.avgRowBytes()
        );

        long timeSpanMs = config.timeRangeMs();
        long globalOffset = 0L;

        for (int chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++) {
            long remainingRows = totalRows - globalOffset;
            if (remainingRows <= 0) {
                break;
            }

            long rowsInChunk = Math.min(rowsPerChunk, remainingRows);
            long chunkStartMs = config.timestampStartEpochMs() + (chunkIndex * timeSpanMs / chunkCount);
            long chunkEndMs = config.timestampStartEpochMs() + ((chunkIndex + 1L) * timeSpanMs / chunkCount);
            String saveMode = chunkIndex == 0 ? "overwrite" : "append";

            LOG.info(
                    "Generating chunk {}/{} rows={} globalOffset={} timeWindow=[{} .. {}] saveMode={}",
                    chunkIndex + 1,
                    chunkCount,
                    rowsInChunk,
                    globalOffset,
                    chunkStartMs,
                    chunkEndMs,
                    saveMode
            );

            Dataset<Row> chunk = DataGenerator.generateChunk(
                    spark,
                    config,
                    chunkIndex,
                    rowsInChunk,
                    globalOffset,
                    chunkStartMs,
                    chunkEndMs
            );

            int writePartitions = estimateWritePartitions(rowsInChunk, config.avgRowBytes(), config.targetFileSizeMb());
            Dataset<Row> prepared = chunk.repartition(writePartitions);

            OrcWriter.write(spark, prepared, config.orcPath(), config.orcWrite(), saveMode);

            globalOffset += rowsInChunk;
        }

        LOG.info("Generation completed: rowsWritten={} orcPath={}", globalOffset, config.orcPath());
    }

    private static int estimateWritePartitions(long rowsInChunk, long avgRowBytes, int targetFileSizeMb) {
        long chunkBytes = rowsInChunk * avgRowBytes;
        long targetFileBytes = targetFileSizeMb * 1024L * 1024L;
        return (int) Math.max(1L, (chunkBytes + targetFileBytes - 1) / targetFileBytes);
    }
}
