package ru.sber.orcbench.benchmark;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.SparkSession;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Captures Spark task input metrics (bytes/records read) for a single action.
 * Useful as a proxy for ORC stripe pruning: selective filters should read fewer bytes than full_scan.
 */
public final class InputMetricsCollector {
    private InputMetricsCollector() {
    }

    public static final class Snapshot {
        private final long bytesRead;
        private final long recordsRead;

        public Snapshot(long bytesRead, long recordsRead) {
            this.bytesRead = bytesRead;
            this.recordsRead = recordsRead;
        }

        public long bytesRead() {
            return bytesRead;
        }

        public long recordsRead() {
            return recordsRead;
        }
    }

    public static final class Measured<T> {
        private final T value;
        private final Snapshot snapshot;

        public Measured(T value, Snapshot snapshot) {
            this.value = value;
            this.snapshot = snapshot;
        }

        public T value() {
            return value;
        }

        public Snapshot snapshot() {
            return snapshot;
        }
    }

    public static <T> Measured<T> measure(SparkSession spark, Supplier<T> action) {
        AtomicLong bytesRead = new AtomicLong();
        AtomicLong recordsRead = new AtomicLong();

        SparkListener listener = new SparkListener() {
            @Override
            public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
                if (taskEnd.taskMetrics() != null) {
                    bytesRead.addAndGet(taskEnd.taskMetrics().inputMetrics().bytesRead());
                    recordsRead.addAndGet(taskEnd.taskMetrics().inputMetrics().recordsRead());
                }
            }
        };

        spark.sparkContext().addSparkListener(listener);
        try {
            T value = action.get();
            return new Measured<>(value, new Snapshot(bytesRead.get(), recordsRead.get()));
        } finally {
            spark.sparkContext().removeSparkListener(listener);
        }
    }
}
