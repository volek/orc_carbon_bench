package ru.sber.orcbench.benchmark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ru.sber.orcbench.config.OutputFormat;

public final class DatasetLoader {
    private DatasetLoader() {
    }

    public static Dataset<Row> load(SparkSession spark, OutputFormat format, String orcPath, String carbonPath) {
        return switch (format) {
            case ORC -> spark.read().orc(orcPath);
            case CARBON -> spark.read().format("carbondata").load(carbonPath);
        };
    }
}
