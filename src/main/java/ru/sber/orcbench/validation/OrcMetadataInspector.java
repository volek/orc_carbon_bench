package ru.sber.orcbench.validation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.RecordReaderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Reads ORC stripe indexes to verify bloom filter presence on configured columns.
 */
public final class OrcMetadataInspector {
    private static final Logger LOG = LoggerFactory.getLogger(OrcMetadataInspector.class);

    private OrcMetadataInspector() {
    }

    public static InspectionResult inspectBloomFilters(
            Configuration conf,
            String orcPath,
            String[] expectedColumns,
            boolean expectPresent
    ) throws IOException {
        Path root = new Path(orcPath);
        FileSystem fs = FileSystem.get(root.toUri(), conf);
        if (!fs.exists(root)) {
            return InspectionResult.fail("ORC path does not exist: " + orcPath);
        }

        List<Path> orcFiles = listOrcFiles(fs, root);
        if (orcFiles.isEmpty()) {
            return InspectionResult.fail("No ORC data files found under " + orcPath);
        }

        Path sampleFile = orcFiles.get(0);
        LOG.info("Inspecting ORC bloom metadata file={} expectPresent={} columns={}",
                sampleFile, expectPresent, Arrays.toString(expectedColumns));

        try (Reader reader = OrcFile.createReader(sampleFile, OrcFile.readerOptions(conf))) {
            int stripeCount = reader.getStripes().size();
            if (stripeCount == 0) {
                return InspectionResult.fail("ORC file has no stripes: " + sampleFile);
            }

            TypeDescription schema = reader.getSchema();
            int[] columnIds = new int[expectedColumns.length];
            for (int i = 0; i < expectedColumns.length; i++) {
                int fieldIndex = schema.getFieldNames().indexOf(expectedColumns[i]);
                if (fieldIndex < 0) {
                    return InspectionResult.fail("Column not found in ORC schema: " + expectedColumns[i]);
                }
                columnIds[i] = schema.getChildren().get(fieldIndex).getId();
            }

            List<String> missing = new ArrayList<>();
            List<String> unexpected = new ArrayList<>();

            try (RecordReader rows = reader.rows()) {
                RecordReaderImpl recordReader = (RecordReaderImpl) rows;
                // ORC loads bloom indexes only for columns marked true in sargColumns.
                // Passing null skips bloom streams entirely and falsely reports them missing.
                boolean[] bloomColumns = bloomColumnMask(schema, columnIds);
                OrcIndex index = recordReader.readRowIndex(0, null, bloomColumns);
                OrcProto.BloomFilterIndex[] bloomIndices = index.getBloomFilterIndex();

                for (int i = 0; i < expectedColumns.length; i++) {
                    boolean present = hasBloomIndex(bloomIndices, columnIds[i]);
                    if (expectPresent && !present) {
                        missing.add(expectedColumns[i]);
                    }
                    if (!expectPresent && present) {
                        unexpected.add(expectedColumns[i]);
                    }
                }
            }

            if (expectPresent && !missing.isEmpty()) {
                return InspectionResult.fail(
                        "Bloom filter missing for columns: " + String.join(",", missing) + " in " + sampleFile
                );
            }
            if (!expectPresent && !unexpected.isEmpty()) {
                return InspectionResult.fail(
                        "Unexpected bloom filter on columns: " + String.join(",", unexpected) + " in " + sampleFile
                );
            }

            String details = "file=" + sampleFile
                    + " stripes=" + stripeCount
                    + " expectPresent=" + expectPresent
                    + " columns=" + Arrays.toString(expectedColumns);
            return InspectionResult.pass(details);
        }
    }

    /**
     * Mask for {@link RecordReaderImpl#readRowIndex}: ORC reads bloom streams only where true.
     */
    static boolean[] bloomColumnMask(TypeDescription schema, int[] columnIds) {
        boolean[] mask = new boolean[schema.getMaximumId() + 1];
        for (int columnId : columnIds) {
            if (columnId >= 0 && columnId < mask.length) {
                mask[columnId] = true;
            }
        }
        return mask;
    }

    private static boolean hasBloomIndex(OrcProto.BloomFilterIndex[] bloomIndices, int columnId) {
        if (bloomIndices == null || columnId < 0 || columnId >= bloomIndices.length) {
            return false;
        }
        OrcProto.BloomFilterIndex index = bloomIndices[columnId];
        return index != null && index.getBloomFilterCount() > 0;
    }

    private static List<Path> listOrcFiles(FileSystem fs, Path root) throws IOException {
        Set<Path> files = new LinkedHashSet<>();
        FileStatus[] statuses = fs.listStatus(root, ORC_FILE_FILTER);
        if (statuses != null) {
            for (FileStatus status : statuses) {
                if (status.isFile()) {
                    files.add(status.getPath());
                } else if (status.isDirectory()) {
                    files.addAll(listOrcFiles(fs, status.getPath()));
                }
            }
        }
        return new ArrayList<>(files);
    }

    private static final PathFilter ORC_FILE_FILTER = path -> {
        String name = path.getName().toLowerCase(Locale.ROOT);
        return !name.startsWith("_") && !name.startsWith(".") && name.contains("part-");
    };

    public static final class InspectionResult {
        private final boolean passed;
        private final String details;

        private InspectionResult(boolean passed, String details) {
            this.passed = passed;
            this.details = details;
        }

        public static InspectionResult pass(String details) {
            return new InspectionResult(true, details);
        }

        public static InspectionResult fail(String details) {
            return new InspectionResult(false, details);
        }

        public boolean passed() {
            return passed;
        }

        public String details() {
            return details;
        }
    }
}
