package ru.sber.orcbench.config;

public final class StoragePaths {
    private final String basePath;
    private final String orcPath;
    private final String reportsPath;
    private final String reportsBenchmarkPath;
    private final String reportsValidationPath;

    public StoragePaths(
            String basePath,
            String orcPath,
            String reportsPath,
            String reportsBenchmarkPath,
            String reportsValidationPath
    ) {
        this.basePath = basePath;
        this.orcPath = orcPath;
        this.reportsPath = reportsPath;
        this.reportsBenchmarkPath = reportsBenchmarkPath;
        this.reportsValidationPath = reportsValidationPath;
    }

    public static StoragePaths from(
            String basePath,
            String orcPath,
            String reportsPath,
            String reportsBenchmarkPath,
            String reportsValidationPath
    ) {
        String normalizedBase = normalize(basePath);
        String normalizedReports = reportsPath != null
                ? normalize(reportsPath)
                : joinPath(normalizedBase, "reports");
        String raw = joinPath(normalizedReports, "raw");
        return new StoragePaths(
                normalizedBase,
                orcPath != null ? normalize(orcPath) : joinPath(normalizedBase, "orc"),
                normalizedReports,
                reportsBenchmarkPath != null
                        ? normalize(reportsBenchmarkPath)
                        : joinPath(raw, "benchmark"),
                reportsValidationPath != null
                        ? normalize(reportsValidationPath)
                        : joinPath(raw, "validation")
        );
    }

    public String basePath() {
        return basePath;
    }

    public String orcPath() {
        return orcPath;
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
