package ru.sber.orcbench.config;

import ru.sber.orcbench.generator.GeneratorConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;

public final class AppConfig {
    private final Mode mode;
    private final StoragePaths paths;
    private final double targetSizeTb;
    private final long seed;
    private final long avgRowBytes;
    private final int chunkDays;
    private final long timestampStartEpochMs;
    private final long timestampEndEpochMs;
    private final int targetFileSizeMb;
    private final OrcWriteSettings orcWrite;
    private final BenchmarkSettings benchmark;
    private final ValidationSettings validation;
    private final ReportSettings report;
    private final String benchmarkDatasetLabel;

    public AppConfig(
            Mode mode,
            StoragePaths paths,
            double targetSizeTb,
            long seed,
            long avgRowBytes,
            int chunkDays,
            long timestampStartEpochMs,
            long timestampEndEpochMs,
            int targetFileSizeMb,
            OrcWriteSettings orcWrite,
            BenchmarkSettings benchmark,
            ValidationSettings validation,
            ReportSettings report,
            String benchmarkDatasetLabel
    ) {
        this.mode = mode;
        this.paths = paths;
        this.targetSizeTb = targetSizeTb;
        this.seed = seed;
        this.avgRowBytes = avgRowBytes;
        this.chunkDays = chunkDays;
        this.timestampStartEpochMs = timestampStartEpochMs;
        this.timestampEndEpochMs = timestampEndEpochMs;
        this.targetFileSizeMb = targetFileSizeMb;
        this.orcWrite = orcWrite;
        this.benchmark = benchmark;
        this.validation = validation;
        this.report = report;
        this.benchmarkDatasetLabel = benchmarkDatasetLabel;
    }

    public static AppConfig fromArgs(String[] args) {
        Map<String, String> kv = parseArgs(args);
        String modeValue = require(kv, "mode");

        long timestampStart = kv.containsKey("timestamp-start")
                ? GeneratorConfig.parseEpochMs(kv.get("timestamp-start"), "timestamp-start")
                : GeneratorConfig.defaultTimestampStart();
        long timestampEnd = kv.containsKey("timestamp-end")
                ? GeneratorConfig.parseEpochMs(kv.get("timestamp-end"), "timestamp-end")
                : GeneratorConfig.defaultTimestampEnd();
        if (timestampEnd <= timestampStart) {
            throw new IllegalArgumentException("--timestamp-end must be greater than --timestamp-start");
        }

        String basePath = kv.getOrDefault("base-path", "hdfs:///user/hdfs_migration_user/orc_test");
        StoragePaths paths = StoragePaths.from(
                basePath,
                kv.get("orc-path"),
                kv.get("reports-path"),
                kv.get("reports-benchmark-path"),
                kv.get("reports-validation-path")
        );

        String[] partitionBy = kv.containsKey("partition-by")
                ? ArgParser.parseCsv(kv.get("partition-by"))
                : OrcWriteSettings.DEFAULT_PARTITION_BY;

        OptionalInt writePartitions = ArgParser.parseOptionalPositiveInt(kv, "write-partitions");
        int partitions = writePartitions.orElse(0);

        String[] bloomColumns = OrcWriteSettings.parseBloomFilterColumns(
                kv.getOrDefault("orc-bloom-filter-columns", String.join(",", OrcWriteSettings.DEFAULT_BLOOM_FILTER_COLUMNS))
        );
        double bloomFpp = OrcWriteSettings.parseBloomFilterFpp(kv.get("orc-bloom-filter-fpp"));

        OrcWriteSettings orcWrite = new OrcWriteSettings(
                ArgParser.parseEnum(kv.getOrDefault("orc-compression", "snappy"), "orc-compression", "snappy", "zstd", "none"),
                (int) ArgParser.parsePositiveLong(kv.getOrDefault("orc-stripe-size-mb", "64"), "orc-stripe-size-mb"),
                (int) ArgParser.parsePositiveLong(kv.getOrDefault("orc-row-group-size-mb", "32"), "orc-row-group-size-mb"),
                partitions,
                partitionBy,
                bloomColumns,
                bloomFpp
        );

        BenchmarkSettings benchmark = new BenchmarkSettings(
                ArgParser.parseNonNegativeInt(kv.getOrDefault("benchmark-warmup-runs", "1"), "benchmark-warmup-runs"),
                ArgParser.parsePositiveInt(kv.getOrDefault("benchmark-repeat-runs", "3"), "benchmark-repeat-runs"),
                kv.containsKey("benchmark-scenarios")
                        ? BenchmarkSettings.parseScenarios(kv.get("benchmark-scenarios"))
                        : BenchmarkSettings.defaults().scenarios(),
                ArgParser.parseBoolean(kv.getOrDefault("clear-cache-between-runs", "true"), "clear-cache-between-runs"),
                ArgParser.parsePositiveInt(
                        kv.getOrDefault(
                                "benchmark-timestamp-window-days",
                                String.valueOf(BenchmarkSettings.DEFAULT_TIMESTAMP_WINDOW_DAYS)
                        ),
                        "benchmark-timestamp-window-days"
                )
        );

        ValidationSettings validation = ValidationSettings.from(kv);
        ReportSettings report = ReportSettings.from(kv);

        String datasetLabel = kv.containsKey("benchmark-dataset-label")
                ? kv.get("benchmark-dataset-label").trim()
                : (orcWrite.bloomFiltersEnabled() ? "bloom" : "nobloom");

        return new AppConfig(
                Mode.fromCli(modeValue),
                paths,
                ArgParser.parsePositiveDouble(kv.getOrDefault("target-size-tb", "5"), "target-size-tb"),
                parseLong(kv.getOrDefault("seed", "42"), "seed"),
                parseLong(kv.getOrDefault("avg-row-bytes", "512"), "avg-row-bytes"),
                (int) parseLong(kv.getOrDefault("chunk-days", "1"), "chunk-days"),
                timestampStart,
                timestampEnd,
                (int) parseLong(kv.getOrDefault("target-file-size-mb", "384"), "target-file-size-mb"),
                orcWrite,
                benchmark,
                validation,
                report,
                datasetLabel
        );
    }

    static String inferBenchmarkDatasetLabel(String orcPath) {
        String normalized = orcPath.replace('\\', '/');
        if (normalized.endsWith("/orc_bloom") || normalized.contains("/orc_bloom/")) {
            return "bloom";
        }
        if (normalized.endsWith("/orc") || normalized.endsWith("/orc/")) {
            return "nobloom";
        }
        return "default";
    }

    /** Used when {@code --benchmark-dataset-label} is omitted: tags from write config. */
    static String inferDatasetLabelFromOrcWrite(OrcWriteSettings orcWrite) {
        return orcWrite.bloomFiltersEnabled() ? "bloom" : "nobloom";
    }

    public Mode mode() {
        return mode;
    }

    public StoragePaths paths() {
        return paths;
    }

    public double targetSizeTb() {
        return targetSizeTb;
    }

    public long seed() {
        return seed;
    }

    public long avgRowBytes() {
        return avgRowBytes;
    }

    public int chunkDays() {
        return chunkDays;
    }

    public long timestampStartEpochMs() {
        return timestampStartEpochMs;
    }

    public long timestampEndEpochMs() {
        return timestampEndEpochMs;
    }

    public int targetFileSizeMb() {
        return targetFileSizeMb;
    }

    public OrcWriteSettings orcWrite() {
        return orcWrite;
    }

    public BenchmarkSettings benchmark() {
        return benchmark;
    }

    public ValidationSettings validation() {
        return validation;
    }

    public ReportSettings report() {
        return report;
    }

    public String benchmarkDatasetLabel() {
        return benchmarkDatasetLabel;
    }

    public String basePath() {
        return paths.basePath();
    }

    public String orcPath() {
        return paths.orcPath();
    }

    public String reportsPath() {
        return paths.reportsPath();
    }

    public String reportsRawPath() {
        return paths.reportsRawPath();
    }

    public String reportsBenchmarkPath() {
        return paths.reportsBenchmarkPath();
    }

    public String reportsSummaryPath() {
        return paths.reportsSummaryPath();
    }

    public String reportsValidationPath() {
        return paths.reportsValidationPath();
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> kv = new HashMap<>();
        for (String arg : args) {
            if (!arg.startsWith("--") || !arg.contains("=")) {
                throw new IllegalArgumentException("Invalid argument: " + arg + ". Use --key=value");
            }
            int split = arg.indexOf('=');
            String key = arg.substring(2, split).trim();
            String value = arg.substring(split + 1).trim();
            if (key.isEmpty() || value.isEmpty()) {
                throw new IllegalArgumentException("Invalid argument: " + arg + ". Key/value must be non-empty");
            }
            kv.put(key, value);
        }
        return kv;
    }

    private static String require(Map<String, String> kv, String key) {
        String value = kv.get(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing required argument: --" + key + "=...");
        }
        return value;
    }

    private static long parseLong(String raw, String key) {
        return ArgParser.parsePositiveLong(raw, key);
    }
}
