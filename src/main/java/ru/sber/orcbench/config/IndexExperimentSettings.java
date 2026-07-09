package ru.sber.orcbench.config;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public record IndexExperimentSettings(
        Set<IndexProfile> profiles,
        Map<IndexProfile, String> carbonPathsByProfile,
        boolean rebuildIndexes,
        int warmupRuns,
        int repeatRuns,
        boolean clearCacheBetweenRuns
) {
    public static IndexExperimentSettings from(
            Map<String, String> kv,
            StoragePaths paths,
            CarbonWriteSettings carbonWrite,
            BenchmarkSettings benchmark
    ) {
        Set<IndexProfile> profiles = kv.containsKey("index-profiles")
                ? IndexProfile.parseCsv(kv.get("index-profiles"))
                : EnumSet.of(IndexProfile.BASELINE, IndexProfile.BLOOM, IndexProfile.LUCENE, IndexProfile.BLOOM_LUCENE);

        Map<IndexProfile, String> carbonPaths = new HashMap<>();
        for (IndexProfile profile : profiles) {
            String key = "carbon-" + profile.cliValue().replace('_', '-') + "-path";
            String profilePath = kv.get(key);
            if (profilePath != null) {
                carbonPaths.put(profile, profilePath);
            }
        }

        if (carbonPaths.isEmpty()) {
            carbonPaths.put(IndexProfile.BASELINE, paths.carbonPath());
            if (carbonWrite.enableBloomIndex()) {
                carbonPaths.put(IndexProfile.BLOOM, paths.carbonPath());
                carbonPaths.put(IndexProfile.BLOOM_LUCENE, paths.carbonPath());
            }
            if (carbonWrite.enableLuceneIndex()) {
                carbonPaths.put(IndexProfile.LUCENE, paths.carbonPath());
                if (!carbonPaths.containsKey(IndexProfile.BLOOM_LUCENE)) {
                    carbonPaths.put(IndexProfile.BLOOM_LUCENE, paths.carbonPath());
                }
            }
        }

        return new IndexExperimentSettings(
                profiles,
                carbonPaths,
                ArgParser.parseBoolean(kv.getOrDefault("rebuild-indexes", "false"), "rebuild-indexes"),
                benchmark.warmupRuns(),
                benchmark.repeatRuns(),
                benchmark.clearCacheBetweenRuns()
        );
    }

    public String resolveCarbonPath(IndexProfile profile, String defaultCarbonPath) {
        return carbonPathsByProfile.getOrDefault(profile, defaultCarbonPath);
    }
}
