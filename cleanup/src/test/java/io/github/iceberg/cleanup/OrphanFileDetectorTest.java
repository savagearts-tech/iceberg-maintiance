package io.github.iceberg.cleanup;

import org.junit.jupiter.api.Test;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;


class OrphanFileDetectorTest {

    private final OrphanFileDetector detector = new OrphanFileDetector();

    @Test
    void dataFileIsOrphan() {
        Set<String> physical = Set.of(
                "s3a://bucket/table/data/event_date=2026-05-01/file_a.parquet",
                "s3a://bucket/table/data/event_date=2026-05-01/file_b.parquet");
        Set<String> referenced = Set.of("s3a://bucket/table/data/event_date=2026-05-01/file_a.parquet");
        Set<String> orphans = detector.detectDataOrphans(physical, referenced);
        assertEquals(Set.of("s3a://bucket/table/data/event_date=2026-05-01/file_b.parquet"), orphans);
    }

    @Test
    void noOrphansWhenAllReferenced() {
        Set<String> physical = Set.of("s3a://bucket/table/data/file.parquet");
        Set<String> referenced = Set.of("s3a://bucket/table/data/file.parquet");
        assertTrue(detector.detectDataOrphans(physical, referenced).isEmpty());
    }

    @Test
    void uriNormalizationApplied() {
        Set<String> physical = Set.of("s3://bucket/table/data/file.parquet");
        Set<String> referenced = Set.of("s3a://bucket/table/data/file.parquet");
        assertTrue(detector.detectDataOrphans(physical, referenced).isEmpty());
    }

    @Test
    void metadataFileIsOrphan() {
        Set<String> physical = Set.of(
                "s3a://bucket/table/metadata/v1.metadata.json",
                "s3a://bucket/table/metadata/v2.metadata.json");
        Set<String> active = Set.of("s3a://bucket/table/metadata/v2.metadata.json");
        Set<String> orphans = detector.detectMetadataOrphans(physical, active);
        assertEquals(Set.of("s3a://bucket/table/metadata/v1.metadata.json"), orphans);
    }

    @Test
    void dataFilesNotInMetadataSet() {
        Set<String> physical = Set.of(
                "s3a://bucket/table/data/file.parquet",
                "s3a://bucket/table/metadata/v1.metadata.json");
        Set<String> active = Set.of("s3a://bucket/table/metadata/v1.metadata.json");
        assertTrue(detector.detectMetadataOrphans(physical, active).isEmpty(),
                "data files should not appear in metadata orphan detection");
    }

    @Test
    void emptyPhysicalSet() {
        assertTrue(detector.detectDataOrphans(Set.of(), Set.of()).isEmpty());
        assertTrue(detector.detectMetadataOrphans(Set.of(), Set.of("a")).isEmpty());
    }
}
