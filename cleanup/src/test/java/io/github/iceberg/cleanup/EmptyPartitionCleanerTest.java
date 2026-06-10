package io.github.iceberg.cleanup;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class EmptyPartitionCleanerTest {

    private static final String DATA_ROOT = "s3a://bucket/db/tbl/data/";

    @Test
    void isDataPartitionPrefix_acceptsHiveStylePath() {
        assertTrue(EmptyPartitionCleaner.isDataPartitionPrefix(
                DATA_ROOT + "event_date_day=20597/", DATA_ROOT));
    }

    @Test
    void isDataPartitionPrefix_rejectsTableRoot() {
        assertFalse(EmptyPartitionCleaner.isDataPartitionPrefix(DATA_ROOT, DATA_ROOT));
    }

    @Test
    void isDataPartitionPrefix_rejectsMetadata() {
        assertFalse(EmptyPartitionCleaner.isDataPartitionPrefix(
                "s3a://bucket/db/tbl/metadata/", DATA_ROOT));
    }

    @Test
    void hasReferencedDataFile_trueWhenPartitionMatches() {
        Set<String> referenced = Set.of("s3a://bucket/db/tbl/data/event_date_day=20597/part-00001.parquet");
        assertTrue(EmptyPartitionCleaner.hasReferencedDataFile(
                DATA_ROOT + "event_date_day=20597/", referenced));
    }

    @Test
    void hasReferencedDataFile_falseWhenPartitionEmpty() {
        Set<String> referenced = Set.of("s3a://bucket/db/tbl/data/event_date_day=20596/part-00001.parquet");
        assertFalse(EmptyPartitionCleaner.hasReferencedDataFile(
                DATA_ROOT + "event_date_day=20597/", referenced));
    }

    @Test
    void collectCandidates_fromScanPrefixesAndDeletedFiles() {
        Set<String> candidates = EmptyPartitionCleaner.collectCandidates(
                DATA_ROOT,
                List.of(
                        DATA_ROOT + "event_date_day=20597/",
                        "s3a://bucket/db/tbl/metadata/"),
                List.of("s3a://bucket/db/tbl/data/event_date_day=20598/orphan.parquet"));

        assertTrue(candidates.contains(DATA_ROOT + "event_date_day=20597/"));
        assertTrue(candidates.contains(DATA_ROOT + "event_date_day=20598/"));
        assertFalse(candidates.stream().anyMatch(p -> p.contains("/metadata/")));
    }

    @Test
    void directoryPrefixForDataFile() {
        assertEquals(
                DATA_ROOT + "event_date_day=20597/",
                EmptyPartitionCleaner.directoryPrefixForDataFile(
                        DATA_ROOT + "event_date_day=20597/part-00001.parquet"));
    }
}
