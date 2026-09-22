package ru.sber.orcbench.validation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrcMetadataInspectorTest {

    @TempDir
    File tempDir;

    @Test
    void detectsBloomWhenPresent() throws Exception {
        Path orcDir = writeDataset(true);
        OrcMetadataInspector.InspectionResult result = OrcMetadataInspector.inspectBloomFilters(
                new Configuration(),
                orcDir.toString(),
                new String[]{"epk_id", "event_id"},
                true
        );
        assertTrue(result.passed(), result.details());
    }

    @Test
    void detectsAbsenceWhenBloomDisabled() throws Exception {
        Path orcDir = writeDataset(false);
        OrcMetadataInspector.InspectionResult result = OrcMetadataInspector.inspectBloomFilters(
                new Configuration(),
                orcDir.toString(),
                new String[]{"epk_id", "event_id"},
                false
        );
        assertTrue(result.passed(), result.details());
    }

    @Test
    void failsWhenBloomExpectedButMissing() throws Exception {
        Path orcDir = writeDataset(false, false);
        OrcMetadataInspector.InspectionResult result = OrcMetadataInspector.inspectBloomFilters(
                new Configuration(),
                orcDir.toString(),
                new String[]{"epk_id"},
                true
        );
        assertFalse(result.passed());
        assertTrue(result.details().contains("Bloom filter missing"));
    }

    @Test
    void findsOrcFilesUnderPartitionDirectories() throws Exception {
        Path orcDir = writeDataset(true, true);
        OrcMetadataInspector.InspectionResult result = OrcMetadataInspector.inspectBloomFilters(
                new Configuration(),
                orcDir.toString(),
                new String[]{"epk_id", "event_id"},
                true
        );
        assertTrue(result.passed(), result.details());
        assertTrue(result.details().contains("event_year=2024"));
    }

    private Path writeDataset(boolean withBloom) throws Exception {
        return writeDataset(withBloom, false);
    }

    private Path writeDataset(boolean withBloom, boolean partitioned) throws Exception {
        File dir = new File(tempDir, (withBloom ? "with_bloom" : "no_bloom")
                + (partitioned ? "_partitioned" : ""));
        File dataDir = partitioned
                ? new File(dir, "event_year=2024/event_month=1/event_day=1")
                : dir;
        Files.createDirectories(dataDir.toPath());
        // Marker files that must be ignored by the inspector
        Files.write(new File(dir, "_SUCCESS").toPath(), new byte[0]);
        Path file = new Path(new File(dataDir, "part-00000.orc").getAbsolutePath());

        TypeDescription schema = TypeDescription.createStruct()
                .addField("epk_id", TypeDescription.createString())
                .addField("event_id", TypeDescription.createString());

        Configuration conf = new Configuration();
        if (withBloom) {
            OrcConf.BLOOM_FILTER_COLUMNS.setString(conf, "epk_id,event_id");
            OrcConf.BLOOM_FILTER_FPP.setDouble(conf, 0.05d);
        }

        try (Writer writer = OrcFile.createWriter(
                file,
                OrcFile.writerOptions(conf)
                        .setSchema(schema)
                        .stripeSize(64 * 1024)
                        .bufferSize(64 * 1024)
                        .rowIndexStride(1000)
        )) {
            VectorizedRowBatch batch = schema.createRowBatch();
            BytesColumnVector epkIds = (BytesColumnVector) batch.cols[0];
            BytesColumnVector eventIds = (BytesColumnVector) batch.cols[1];
            for (int i = 0; i < 2000; i++) {
                int row = batch.size++;
                byte[] epk = ("epk-" + i).getBytes(StandardCharsets.UTF_8);
                byte[] evt = ("evt-" + i).getBytes(StandardCharsets.UTF_8);
                epkIds.setVal(row, epk);
                eventIds.setVal(row, evt);
                if (batch.size == batch.getMaxSize()) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }
            if (batch.size != 0) {
                writer.addRowBatch(batch);
            }
        }
        return new Path(dir.getAbsolutePath());
    }
}
