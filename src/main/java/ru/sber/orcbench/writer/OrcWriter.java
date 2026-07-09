package ru.sber.orcbench.writer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sber.orcbench.config.OrcWriteSettings;

public final class OrcWriter {
    private static final Logger LOG = LoggerFactory.getLogger(OrcWriter.class);

    private OrcWriter() {
    }

    public static void configureSpark(SparkSession spark, OrcWriteSettings settings) {
        spark.conf().set("spark.sql.orc.filterPushdown", "true");
        spark.conf().set("spark.sql.orc.enableVectorizedReader", "true");
        spark.conf().set("spark.sql.orc.block.size", String.valueOf(settings.stripeSizeMb() * 1024L * 1024L));
        spark.conf().set("spark.sql.orc.row.index.stride", String.valueOf(settings.rowGroupSizeMb() * 1024 * 1024 / 4));
    }

    public static void write(SparkSession spark, Dataset<Row> dataset, String orcPath, OrcWriteSettings settings, String saveMode) {
        configureSpark(spark, settings);

        Dataset<Row> toWrite = settings.hasExplicitWritePartitions()
                ? dataset.repartition(settings.writePartitions())
                : dataset;

        LOG.info(
                "Writing ORC: path={} mode={} compression={} partitionBy={}",
                orcPath,
                saveMode,
                settings.compression(),
                String.join(",", settings.partitionBy())
        );

        toWrite.write()
                .mode(saveMode)
                .option("compression", settings.compression())
                .partitionBy(settings.partitionBy())
                .orc(orcPath);
    }
}
