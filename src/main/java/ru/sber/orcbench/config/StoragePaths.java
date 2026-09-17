package ru.sber.orcbench.config;

public final class StoragePaths {
    private final String basePath;
    private final String orcPath;
    private final String reportsPath;
    private final String reportsBenchmarkPath;
    private final String reportsValidationPath;
    private final String dictionaryPath;
    private final String layoutId;

    public StoragePaths(
            String basePath,
            String orcPath,
            String reportsPath,
            String reportsBenchmarkPath,
            String reportsValidationPath
    ) {
        this(basePath, orcPath, reportsPath, reportsBenchmarkPath, reportsValidationPath, null, "default");
    }

    public StoragePaths(
            String basePath,
            String orcPath,
            String reportsPath,
            String reportsBenchmarkPath,
            String reportsValidationPath,
            String dictionaryPath,
            String layoutId
    ) {
        this.basePath = basePath;
        this.orcPath = orcPath;
        this.reportsPath = reportsPath;
        this.reportsBenchmarkPath = reportsBenchmarkPath;
        this.reportsValidationPath = reportsValidationPath;
        this.dictionaryPath = dictionaryPath;
        this.layoutId = layoutId;
    }

    public static StoragePaths from(
            String basePath,
            String orcPath,
            String reportsPath,
            String reportsBenchmarkPath,
            String reportsValidationPath
    ) {
        return from(basePath, orcPath, reportsPath, reportsBenchmarkPath, reportsValidationPath, null, "default");
    }

    /**
     * Resolves paths. When {@code layoutId} is not {@code default} and orc/reports paths
     * are omitted, uses {@code <base>/layouts/<layoutId>/orc} and nested reports.
     */
    public static StoragePaths from(
            String basePath,
            String orcPath,
            String reportsPath,
            String reportsBenchmarkPath,
            String reportsValidationPath,
            String dictionaryPath,
            String layoutId
    ) {
        String normalizedBase = normalize(basePath);
        String resolvedLayout = layoutId == null || layoutId.trim().isEmpty() ? "default" : layoutId.trim();
        boolean layoutScoped = !"default".equals(resolvedLayout);

        String layoutRoot = layoutScoped
                ? joinPath(joinPath(normalizedBase, "layouts"), resolvedLayout)
                : normalizedBase;

        String resolvedOrc = orcPath != null
                ? normalize(orcPath)
                : joinPath(layoutRoot, "orc");

        String normalizedReports = reportsPath != null
                ? normalize(reportsPath)
                : joinPath(layoutRoot, "reports");
        String raw = joinPath(normalizedReports, "raw");

        String resolvedDictionary = dictionaryPath != null
                ? normalize(dictionaryPath)
                : joinPath(layoutRoot, "dictionary");

        return new StoragePaths(
                normalizedBase,
                resolvedOrc,
                normalizedReports,
                reportsBenchmarkPath != null
                        ? normalize(reportsBenchmarkPath)
                        : joinPath(raw, "benchmark"),
                reportsValidationPath != null
                        ? normalize(reportsValidationPath)
                        : joinPath(raw, "validation"),
                resolvedDictionary,
                resolvedLayout
        );
    }

    public String basePath() {
        return basePath;
    }

    public String orcPath() {
        return orcPath;
    }

    public String layoutId() {
        return layoutId;
    }

    public String dictionaryPath() {
        return dictionaryPath;
    }

    /** Default bloom-enabled dataset path for A/B runs. */
    public String orcBloomPath() {
        return joinPath(basePath, "orc_bloom");
    }

    public String reportsPath() {
        return reportsPath;
    }

    public String reportsRawPath() {
        return joinPath(reportsPath, "raw");
    }

    public String reportsBenchmarkPath() {
        return reportsBenchmarkPath;
    }

    public String reportsBenchmarkNobloomPath() {
        return joinPath(reportsRawPath(), "benchmark_nobloom");
    }

    public String reportsBenchmarkBloomPath() {
        return joinPath(reportsRawPath(), "benchmark_bloom");
    }

    public String reportsValidationPath() {
        return reportsValidationPath;
    }

    public String reportsValidationNobloomPath() {
        return joinPath(reportsRawPath(), "validation_nobloom");
    }

    public String reportsValidationBloomPath() {
        return joinPath(reportsRawPath(), "validation_bloom");
    }

    public String reportsSummaryPath() {
        return joinPath(reportsPath, "summary");
    }

    /** Layout-scoped report raw dir under a named factor run, e.g. {@code cold}. */
    public String reportsBenchmarkFor(String suffix) {
        return joinPath(reportsRawPath(), "benchmark_" + suffix);
    }

    private static String normalize(String path) {
        if (path.endsWith("/") && path.length() > 1) {
            return path.substring(0, path.length() - 1);
        }
        return path;
    }

    private static String joinPath(String base, String child) {
        if (base.endsWith("/")) {
            return base + child;
        }
        return base + "/" + child;
    }
}
