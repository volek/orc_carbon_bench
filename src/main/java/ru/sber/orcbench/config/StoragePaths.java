package ru.sber.orcbench.config;

public record StoragePaths(
        String basePath,
        String orcPath,
        String carbonPath,
        String reportsPath
) {
    public static StoragePaths from(String basePath, String orcPath, String carbonPath, String reportsPath) {
        String normalizedBase = normalize(basePath);
        return new StoragePaths(
                normalizedBase,
                orcPath != null ? normalize(orcPath) : joinPath(normalizedBase, "orc"),
                carbonPath != null ? normalize(carbonPath) : joinPath(normalizedBase, "carbon"),
                reportsPath != null ? normalize(reportsPath) : joinPath(normalizedBase, "reports")
        );
    }

    public String reportsRawPath() {
        return joinPath(reportsPath, "raw");
    }

    public String reportsIndexPath() {
        return joinPath(reportsRawPath(), "index");
    }

    public String reportsIndexBuildPath() {
        return joinPath(reportsIndexPath(), "build-metrics");
    }

    public String reportsValidationPath() {
        return joinPath(reportsRawPath(), "validation");
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
