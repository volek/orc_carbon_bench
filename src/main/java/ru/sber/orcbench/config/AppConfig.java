package ru.sber.orcbench.config;

import ru.sber.orcbench.generator.GeneratorConfig;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;

public final class AppConfig {
    private final Mode mode;
    private final StoragePaths paths;
    private final Set<OutputFormat> outputFormats;
    private final long targetSizeTb;
    private final long seed;
    private final long avgRowBytes;
    private final int chunkDays;
    private final long timestampStartEpochMs;
    private final long timestampEndEpochMs;
    private final int targetFileSizeMb;
    private final OrcWriteSettings orcWrite;
    private final CarbonWriteSettings carbonWrite;
    private final BenchmarkSettings benchmark;
    private final IndexExperimentSettings indexExperiment;
    private final ValidationSettings validation;
    private final ReportSettings report;

    public AppConfig(
            Mode mode,
            StoragePaths paths,
            Set<OutputFormat> outputFormats,
            long targetSizeTb,
            long seed,
            long avgRowBytes,
            int chunkDays,
            long timestampStartEpochMs,
            long timestampEndEpochMs,
            int targetFileSizeMb,
            OrcWriteSettings orcWrite,
            CarbonWriteSettings carbonWrite,
            BenchmarkSettings benchmark,
            IndexExperimentSettings indexExperiment,
            ValidationSettings validation,
            ReportSettings report
    ) {
        this.mode = mode;
        this.paths = paths;
        this.outputFormats = outputFormats;
        this.targetSizeTb = targetSizeTb;
        this.seed = seed;
        this.avgRowBytes = avgRowBytes;
        this.chunkDays = chunkDays;
        this.timestampStartEpochMs = timestampStartEpochMs;
        this.timestampEndEpochMs = timestampEndEpochMs;
        this.targetFileSizeMb = targetFileSizeMb;
        this.orcWrite = orcWrite;
        this.carbonWrite = carbonWrite;
        this.benchmark = benchmark;
        this.indexExperiment = indexExperiment;
        this.validation = validation;
        this.report = report;
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

        String basePath = kv.getOrDefault("base-path", "/bench/orc-carbon");
        StoragePaths paths = StoragePaths.from(
                basePath,
                kv.get("orc-path"),
                kv.get("carbon-path"),
                kv.get("reports-path")
        );

        String[] partitionBy = kv.containsKey("partition-by")
                ? ArgParser.parseCsv(kv.get("partition-by"))
                : OrcWriteSettings.DEFAULT_PARTITION_BY;

        OptionalInt writePartitions = ArgParser.parseOptionalPositiveInt(kv, "write-partitions");
        int partitions = writePartitions.orElse(0);

        OrcWriteSettings orcWrite = new OrcWriteSettings(
                ArgParser.parseEnum(kv.getOrDefault("orc-compression", "snappy"), "orc-compression", "snappy", "zstd", "none"),
                (int) ArgParser.parsePositiveLong(kv.getOrDefault("orc-stripe-size-mb", "64"), "orc-stripe-size-mb"),
                (int) ArgParser.parsePositiveLong(kv.getOrDefault("orc-row-group-size-mb", "32"), "orc-row-group-size-mb"),
                partitions,
                partitionBy
        );

        CarbonWriteSettings carbonWrite = new CarbonWriteSettings(
                kv.getOrDefault("carbon-table-name", "bench_events"),
                ArgParser.parseEnum(kv.getOrDefault("carbon-compression", "snappy"), "carbon-compression", "snappy", "zstd", "none"),
                ArgParser.parseBoolean(kv.getOrDefault("enable-bloom-index", "false"), "enable-bloom-index"),
                kv.containsKey("bloom-index-columns")
                        ? ArgParser.parseCsv(kv.get("bloom-index-columns"))
                        : CarbonWriteSettings.DEFAULT_BLOOM_COLUMNS,
                ArgParser.parseBoolean(kv.getOrDefault("enable-lucene-index", "false"), "enable-lucene-index"),
                kv.containsKey("lucene-index-columns")
                        ? ArgParser.parseCsv(kv.get("lucene-index-columns"))
                        : CarbonWriteSettings.DEFAULT_LUCENE_COLUMNS,
                partitions,
                partitionBy
        );

        Set<OutputFormat> outputFormats = kv.containsKey("output-formats")
                ? OutputFormat.parseCsv(kv.get("output-formats"))
                : EnumSet.of(OutputFormat.ORC, OutputFormat.CARBON);

        BenchmarkSettings benchmark = new BenchmarkSettings(
                ArgParser.parseNonNegativeInt(kv.getOrDefault("benchmark-warmup-runs", "1"), "benchmark-warmup-runs"),
                ArgParser.parsePositiveInt(kv.getOrDefault("benchmark-repeat-runs", "3"), "benchmark-repeat-runs"),
                kv.containsKey("benchmark-scenarios")
                        ? BenchmarkSettings.parseScenarios(kv.get("benchmark-scenarios"))
                        : BenchmarkSettings.defaults().scenarios(),
                ArgParser.parseBoolean(kv.getOrDefault("clear-cache-between-runs", "true"), "clear-cache-between-runs"),
                kv.containsKey("formats")
                        ? BenchmarkSettings.parseFormats(kv.get("formats"))
                        : EnumSet.of(OutputFormat.ORC, OutputFormat.CARBON)
        );

        IndexExperimentSettings indexExperiment = IndexExperimentSettings.from(kv, paths, carbonWrite, benchmark);
        ValidationSettings validation = ValidationSettings.from(kv);
        ReportSettings report = ReportSettings.from(kv);

        return new AppConfig(
                Mode.fromCli(modeValue),
                paths,
                outputFormats,
                parseLong(kv.getOrDefault("target-size-tb", "5"), "target-size-tb"),
                parseLong(kv.getOrDefault("seed", "42"), "seed"),
                parseLong(kv.getOrDefault("avg-row-bytes", "512"), "avg-row-bytes"),
                (int) parseLong(kv.getOrDefault("chunk-days", "1"), "chunk-days"),
                timestampStart,
                timestampEnd,
                (int) parseLong(kv.getOrDefault("target-file-size-mb", "384"), "target-file-size-mb"),
                orcWrite,
                carbonWrite,
                benchmark,
                indexExperiment,
                validation,
                report
        );
    }

    public Mode mode() {
        return mode;
    }

    public StoragePaths paths() {
        return paths;
    }

    public Set<OutputFormat> outputFormats() {
        return outputFormats;
    }

    public long targetSizeTb() {
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

    public CarbonWriteSettings carbonWrite() {
        return carbonWrite;
    }

    public BenchmarkSettings benchmark() {
        return benchmark;
    }

    public IndexExperimentSettings indexExperiment() {
        return indexExperiment;
    }

    public ValidationSettings validation() {
        return validation;
    }

    public ReportSettings report() {
        return report;
    }

    public String basePath() {
        return paths.basePath();
    }

    public String orcPath() {
        return paths.orcPath();
    }

    public String carbonPath() {
        return paths.carbonPath();
    }

    public String reportsPath() {
        return paths.reportsPath();
    }

    public String reportsRawPath() {
        return paths.reportsRawPath();
    }

    public String reportsSummaryPath() {
        return paths.reportsSummaryPath();
    }

    public String reportsIndexPath() {
        return paths.reportsIndexPath();
    }

    public String reportsIndexBuildPath() {
        return paths.reportsIndexBuildPath();
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
