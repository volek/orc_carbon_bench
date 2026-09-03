package ru.sber.orcbench.validation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
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
                new String[]{"event_id", "user_id"},
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
                new String[]{"event_id", "user_id"},
                false
        );
        assertTrue(result.passed(), result.details());
    }

    @Test
    void failsWhenBloomExpectedButMissing() throws Exception {
        Path orcDir = writeDataset(false);
        OrcMetadataInspector.InspectionResult result = OrcMetadataInspector.inspectBloomFilters(
                new Configuration(),
                orcDir.toString(),
                new String[]{"event_id"},
                true
        );
        assertFalse(result.passed());
        assertTrue(result.details().contains("Bloom filter missing"));
    }

    private Path writeDataset(boolean withBloom) throws Exception {
        File dir = new File(tempDir, withBloom ? "with_bloom" : "no_bloom");
        Files.createDirectories(dir.toPath());
        // Inspector only lists files whose names contain "part-"
        Path file = new Path(new File(dir, "part-00000.orc").getAbsolutePath());

        TypeDescription schema = TypeDescription.createStruct()
                .addField("event_id", TypeDescription.createString())
                .addField("user_id", TypeDescription.createLong());

        Configuration conf = new Configuration();
        if (withBloom) {
            OrcConf.BLOOM_FILTER_COLUMNS.setString(conf, "event_id,user_id");
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
            BytesColumnVector eventIds = (BytesColumnVector) batch.cols[0];
            LongColumnVector userIds = (LongColumnVector) batch.cols[1];
            for (int i = 0; i < 2000; i++) {
                int row = batch.size++;
                byte[] bytes = ("evt-" + i).getBytes(StandardCharsets.UTF_8);
                eventIds.setVal(row, bytes);
                userIds.vector[row] = i;
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
