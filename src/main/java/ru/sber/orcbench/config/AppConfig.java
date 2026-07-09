package ru.sber.orcbench.config;

import ru.sber.orcbench.generator.GeneratorConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;

public record AppConfig(
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
                : Set.of(OutputFormat.ORC, OutputFormat.CARBON);

        BenchmarkSettings benchmark = new BenchmarkSettings(
                ArgParser.parseNonNegativeInt(kv.getOrDefault("benchmark-warmup-runs", "1"), "benchmark-warmup-runs"),
                ArgParser.parsePositiveInt(kv.getOrDefault("benchmark-repeat-runs", "3"), "benchmark-repeat-runs"),
                kv.containsKey("benchmark-scenarios")
                        ? BenchmarkSettings.parseScenarios(kv.get("benchmark-scenarios"))
                        : BenchmarkSettings.defaults().scenarios(),
                ArgParser.parseBoolean(kv.getOrDefault("clear-cache-between-runs", "true"), "clear-cache-between-runs"),
                kv.containsKey("formats")
                        ? BenchmarkSettings.parseFormats(kv.get("formats"))
                        : Set.of(OutputFormat.ORC, OutputFormat.CARBON)
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
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required argument: --" + key + "=...");
        }
        return value;
    }

    private static long parseLong(String raw, String key) {
        return ArgParser.parsePositiveLong(raw, key);
    }
}
