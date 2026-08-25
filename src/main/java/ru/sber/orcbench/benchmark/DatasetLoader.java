package ru.sber.orcbench.benchmark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public final class DatasetLoader {
    private DatasetLoader() {
    }

    public static Dataset<Row> load(SparkSession spark, String orcPath) {
        return spark.read().orc(orcPath);
    }
}
