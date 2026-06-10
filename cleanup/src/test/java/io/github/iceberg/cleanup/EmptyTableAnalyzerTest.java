package io.github.iceberg.cleanup;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class EmptyTableAnalyzerTest {

    private static final String DATA_ROOT = "s3a://bucket/db/tbl/data/";


    @Test
    void analyze_eligibleWhenEmptyAndNoRefs() {
        EmptyTableAnalyzer.Assessment a = EmptyTableAnalyzer.analyze(
                DATA_ROOT,
                Collections.emptySet(),
                Set.of("s3a://bucket/db/tbl/metadata/v1.json"));

        assertTrue(a.eligibleForTableCleanup());
        assertFalse(a.hasReferencedData());
        assertEquals(0, a.physicalDataFileCount());
        assertEquals(1, a.physicalMetadataFileCount());
        assertEquals(0, a.remainingPartitionCount());
    }

    @Test
    void analyze_notEligibleWhenHasRefs() {
        EmptyTableAnalyzer.Assessment a = EmptyTableAnalyzer.analyze(
                DATA_ROOT,
                Set.of("s3a://bucket/db/tbl/data/f1.parquet"),
                Set.of("s3a://bucket/db/tbl/metadata/v1.json"));

        assertFalse(a.eligibleForTableCleanup());
        assertTrue(a.hasReferencedData());
        assertEquals(0, a.physicalDataFileCount());
        assertEquals(1, a.physicalMetadataFileCount());
    }

    @Test
    void analyze_notEligibleWhenHasPhysicalData() {
        EmptyTableAnalyzer.Assessment a = EmptyTableAnalyzer.analyze(
                DATA_ROOT,
                Collections.emptySet(),
                Set.of(
                        "s3a://bucket/db/tbl/data/orphan.parquet",
                        "s3a://bucket/db/tbl/metadata/v1.json"));

        assertFalse(a.eligibleForTableCleanup());
        assertEquals(1, a.physicalDataFileCount());
        assertEquals(1, a.physicalMetadataFileCount());
    }

    @Test
    void analyze_notEligibleWhenHasRemainingPartitions() {
        EmptyTableAnalyzer.Assessment a = EmptyTableAnalyzer.analyze(
                DATA_ROOT,
                Collections.emptySet(),
                Set.of(
                        "s3a://bucket/db/tbl/data/event_date_day=20597/",
                        "s3a://bucket/db/tbl/metadata/v1.json"));

        assertFalse(a.eligibleForTableCleanup());
        assertEquals(0, a.physicalDataFileCount());
        assertEquals(1, a.remainingPartitionCount());
        assertTrue(a.hasUncleanedPartitions());
        assertFalse(a.hasBlockingPartitions());
    }

    @Test
    void analyze_blockingPartitionPreventsTableCleanup() {
        EmptyTableAnalyzer.Assessment a = EmptyTableAnalyzer.analyze(
                DATA_ROOT,
                Set.of("s3a://bucket/db/tbl/data/event_date_day=20597/part-001.parquet"),
                Set.of("s3a://bucket/db/tbl/data/event_date_day=20597/", "s3a://bucket/db/tbl/data/event_date_day=20597/part-001.parquet"));

        assertFalse(a.eligibleForTableCleanup());
        assertEquals(1, a.remainingPartitionCount());
        assertEquals(1, a.blockingPartitionCount());
        assertTrue(a.hasBlockingPartitions());
        assertFalse(a.hasUncleanedPartitions());
    }

    @Test
    void extractPartitionPrefix_valid() {
        assertEquals("s3a://bucket/db/tbl/data/event_date_day=20597/",
                EmptyTableAnalyzer.extractPartitionPrefix(
                        "s3a://bucket/db/tbl/data/event_date_day=20597/",
                        DATA_ROOT, "db/tbl/data/"));
    }

    @Test
    void tableRootPrefixFromDataPath() {
        assertEquals("s3a://bucket/warehouse/ns/tbl/",
                EmptyTableAnalyzer.tableRootPrefix("s3a://bucket/warehouse/ns/tbl/data/"));
    }

    @Test
    void discoverPartitionPrefixesFromPhysicalPaths() {
        Set<String> partitions = EmptyTableAnalyzer.discoverRemainingPartitions(
                DATA_ROOT,
                Set.of(
                        DATA_ROOT + "year=2026/month=05/day=26/part.parquet",
                        DATA_ROOT + "event_date_day=1/"));
        assertEquals(2, partitions.size());
        assertTrue(partitions.contains(DATA_ROOT + "year=2026/month=05/day=26/"));
        assertTrue(partitions.contains(DATA_ROOT + "event_date_day=1/"));
    }
}
