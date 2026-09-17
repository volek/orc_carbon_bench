package ru.sber.orcbench.writer;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sber.orcbench.config.OrcWriteSettings;
import ru.sber.orcbench.config.SparkExecSettings;

public final class OrcWriter {
    private static final Logger LOG = LoggerFactory.getLogger(OrcWriter.class);

    private OrcWriter() {
    }

    public static void configureSpark(SparkSession spark, OrcWriteSettings settings) {
        configureSpark(spark, settings, SparkExecSettings.defaults());
    }

    public static void configureSpark(SparkSession spark, OrcWriteSettings settings, SparkExecSettings exec) {
        spark.conf().set("spark.sql.orc.filterPushdown", String.valueOf(exec.filterPushdown()));
        spark.conf().set("spark.sql.orc.enableVectorizedReader", String.valueOf(exec.vectorizedReader()));
        spark.conf().set("spark.sql.orc.splits.include.file.footer", "true");
        spark.conf().set("spark.sql.orc.cache.stripe.details.size", "10000");
        spark.conf().set("spark.sql.orc.block.size", String.valueOf(settings.stripeSizeMb() * 1024L * 1024L));
        spark.conf().set("spark.sql.orc.row.index.stride", String.valueOf(settings.rowIndexStride()));

        spark.conf().set("spark.sql.adaptive.enabled", String.valueOf(exec.adaptiveExecution()));
        spark.conf().set(
                "spark.sql.optimizer.dynamicPartitionPruning.enabled",
                String.valueOf(exec.dynamicPartitionPruning())
        );
        spark.conf().set("spark.sql.cbo.enabled", String.valueOf(exec.costBasedOptimization()));
        spark.conf().set("spark.sql.cbo.joinReorder.enabled", String.valueOf(exec.costBasedOptimization()));
    }

    public static void write(SparkSession spark, Dataset<Row> dataset, String orcPath, OrcWriteSettings settings, String saveMode) {
        configureSpark(spark, settings);

        Dataset<Row> toWrite = settings.hasExplicitWritePartitions()
                ? dataset.repartition(settings.writePartitions())
                : dataset;

        if (settings.sorted()) {
            Column[] sortCols = new Column[settings.sortColumns().length];
            for (int i = 0; i < settings.sortColumns().length; i++) {
                sortCols[i] = org.apache.spark.sql.functions.col(settings.sortColumns()[i]);
            }
            toWrite = toWrite.sortWithinPartitions(sortCols);
            LOG.info("Applying sortWithinPartitions columns={}", settings.sortColumnsCsv());
        }

        LOG.info(
                "Writing ORC: path={} mode={} compression={} partitionBy={} bloomColumns={} bloomFpp={} stride={} sort={}",
                orcPath,
                saveMode,
                settings.compression(),
                settings.partitioned() ? String.join(",", settings.partitionBy()) : "none",
                settings.bloomFiltersEnabled() ? settings.bloomFilterColumnsCsv() : "none",
                settings.bloomFilterFpp(),
                settings.rowIndexStride(),
                settings.sortColumnsCsv()
        );

        org.apache.spark.sql.DataFrameWriter<Row> writer = toWrite.write()
                .mode(saveMode)
                .option("compression", settings.compression());

        if (settings.partitioned()) {
            writer = writer.partitionBy(settings.partitionBy());
        }

        if (settings.bloomFiltersEnabled()) {
            writer = writer
                    .option("orc.bloom.filter.columns", settings.bloomFilterColumnsCsv())
                    .option("orc.bloom.filter.fpp", String.valueOf(settings.bloomFilterFpp()));
        }

        // ORC Java writer also honors orc.row.index.stride via Hadoop conf / Spark.
        writer = writer.option("orc.row.index.stride", String.valueOf(settings.rowIndexStride()));

        writer.orc(orcPath);
    }
}
