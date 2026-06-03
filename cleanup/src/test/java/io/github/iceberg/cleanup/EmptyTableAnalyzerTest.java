package io.github.iceberg.cleanup;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class EmptyTableAnalyzerTest {

    private static final String DATA_ROOT = "s3a://bucket/db/tbl/data/";

    @Test
    void eligibleWhenNoDataAndNoPartitions() {
        var assessment = EmptyTableAnalyzer.analyze(
                DATA_ROOT,
                Set.of(),
                Set.of("s3a://bucket/db/tbl/metadata/v1.metadata.json"));
        assertTrue(assessment.eligibleForTableCleanup());
        assertEquals(0, assessment.remainingPartitionCount());
        assertEquals(0, assessment.blockingPartitionCount());
    }

    @Test
    void notEligibleWhenReferencedDataExists() {
        var assessment = EmptyTableAnalyzer.analyze(
                DATA_ROOT,
                Set.of(DATA_ROOT + "event_date_day=1/part.parquet"),
                Set.of(DATA_ROOT + "event_date_day=1/part.parquet"));
        assertFalse(assessment.eligibleForTableCleanup());
        assertTrue(assessment.hasReferencedData());
        assertTrue(assessment.hasBlockingPartitions());
    }

    @Test
    void notEligibleWhenOrphanDataRemainsOnStorage() {
        var assessment = EmptyTableAnalyzer.analyze(
                DATA_ROOT,
                Set.of(),
                Set.of(DATA_ROOT + "event_date_day=1/orphan.parquet"));
        assertFalse(assessment.eligibleForTableCleanup());
        assertEquals(1, assessment.physicalDataFileCount());
        assertTrue(assessment.hasBlockingPartitions());
    }

    @Test
    void notEligibleWhenPartitionDirectoryStillOnStorage() {
        var assessment = EmptyTableAnalyzer.analyze(
                DATA_ROOT,
                Set.of(),
                Set.of("s3a://bucket/db/tbl/data/event_date_day=1/"));
        assertFalse(assessment.eligibleForTableCleanup());
        assertEquals(1, assessment.remainingPartitionCount());
        assertEquals(0, assessment.blockingPartitionCount());
        assertTrue(assessment.hasUncleanedPartitions());
    }

    @Test
    void notEligibleWhenPartitionHasDataEvenWithoutRefs() {
        var assessment = EmptyTableAnalyzer.analyze(
                DATA_ROOT,
                Set.of(),
                Set.of(
                        DATA_ROOT + "event_date_day=1/orphan.parquet",
                        DATA_ROOT + "event_date_day=2/"));
        assertFalse(assessment.eligibleForTableCleanup());
        assertEquals(2, assessment.remainingPartitionCount());
        assertEquals(1, assessment.blockingPartitionCount());
    }

    @Test
    void eligibleWhenMetadataScanPrefixExistsButNoPhysicalPartition() {
        var assessment = EmptyTableAnalyzer.analyze(
                DATA_ROOT,
                Set.of(),
                Set.of("s3a://bucket/db/tbl/metadata/v1.metadata.json"));
        assertTrue(assessment.eligibleForTableCleanup());
        assertEquals(0, assessment.remainingPartitionCount());
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
