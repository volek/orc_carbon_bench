package ru.sber.orcbench.config;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum IndexProfile {
    BASELINE("baseline", false, false),
    BLOOM("bloom", true, false),
    LUCENE("lucene", false, true),
    BLOOM_LUCENE("bloom_lucene", true, true);

    private static final Map<String, IndexProfile> BY_CLI = Arrays.stream(values())
            .collect(Collectors.toMap(p -> p.cliValue, Function.identity()));

    private final String cliValue;
    private final boolean bloomEnabled;
    private final boolean luceneEnabled;

    IndexProfile(String cliValue, boolean bloomEnabled, boolean luceneEnabled) {
        this.cliValue = cliValue;
        this.bloomEnabled = bloomEnabled;
        this.luceneEnabled = luceneEnabled;
    }

    public String cliValue() {
        return cliValue;
    }

    public boolean bloomEnabled() {
        return bloomEnabled;
    }

    public boolean luceneEnabled() {
        return luceneEnabled;
    }

    public static IndexProfile fromCli(String value) {
        IndexProfile profile = BY_CLI.get(value.trim().toLowerCase(Locale.ROOT));
        if (profile == null) {
            throw new IllegalArgumentException("Unknown index profile: " + value);
        }
        return profile;
    }

    public static java.util.Set<IndexProfile> parseCsv(String raw) {
        java.util.EnumSet<IndexProfile> profiles = java.util.EnumSet.noneOf(IndexProfile.class);
        for (String part : ArgParser.parseCsv(raw)) {
            profiles.add(fromCli(part));
        }
        if (profiles.isEmpty()) {
            throw new IllegalArgumentException("Invalid argument for --index-profiles: empty list");
        }
        return profiles;
    }

    public CarbonWriteSettings applyTo(CarbonWriteSettings base) {
        return new CarbonWriteSettings(
                base.tableName(),
                base.compression(),
                bloomEnabled,
                base.bloomIndexColumns(),
                luceneEnabled,
                base.luceneIndexColumns(),
                base.writePartitions(),
                base.partitionBy()
        );
    }
}
