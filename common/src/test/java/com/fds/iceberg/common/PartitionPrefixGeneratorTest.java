package com.fds.iceberg.common;

import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class PartitionPrefixGeneratorTest {

    @Test
    void singleLevelPrefix() {
        var gen = new PartitionPrefixGenerator("s3a://bucket/table/data/", List.of("event_date"));
        assertEquals("s3a://bucket/table/data/event_date=2026-05-01/", gen.prefixFor(List.of("2026-05-01")));
    }

    @Test
    void multiLevelPrefix() {
        var gen = new PartitionPrefixGenerator("s3a://bucket/table/data/", List.of("year", "month", "day"));
        assertEquals("s3a://bucket/table/data/year=2026/month=05/day=26/", gen.prefixFor(List.of("2026", "05", "26")));
    }

    @Test
    void prefixFromPartitionPath() {
        var gen = new PartitionPrefixGenerator("s3a://bucket/table/data/", List.of("event_date_day"));
        assertEquals("s3a://bucket/table/data/event_date_day=20597/",
                gen.prefixFromPartitionPath("event_date_day=20597"));
    }

    @Test
    void prefixFromMultiLevelPartitionPath() {
        var gen = new PartitionPrefixGenerator("s3a://bucket/table/data/", List.of("year", "month", "day"));
        assertEquals("s3a://bucket/table/data/year=2026/month=05/day=26/",
                gen.prefixFromPartitionPath("year=2026/month=05/day=26"));
    }

    @Test
    void multiplePrefixes() {
        var gen = new PartitionPrefixGenerator("s3a://bucket/table/data/", List.of("event_date"));
        var prefixes = gen.prefixesFor(List.of(List.of("2026-05-01"), List.of("2026-05-02")));
        assertEquals(2, prefixes.size());
        assertTrue(prefixes.contains("s3a://bucket/table/data/event_date=2026-05-01/"));
        assertTrue(prefixes.contains("s3a://bucket/table/data/event_date=2026-05-02/"));
    }

    @Test
    void fullPrefix() {
        var gen = new PartitionPrefixGenerator("s3a://bucket/table/data", List.of("event_date"));
        assertEquals("s3a://bucket/table/data/", gen.fullPrefix());
    }

    @Test
    void normalizeS3Prefix() {
        var gen = new PartitionPrefixGenerator("s3://bucket/table/data", List.of("c"));
        assertEquals("s3a://bucket/table/data/c=val/", gen.prefixFor(List.of("val")));
    }

    @Test
    void normalizeBarePrefix() {
        var gen = new PartitionPrefixGenerator("bucket/table/data", List.of("c"));
        assertEquals("s3a://bucket/table/data/c=val/", gen.prefixFor(List.of("val")));
    }

    @Test
    void partitionCountMismatchThrows() {
        var gen = new PartitionPrefixGenerator("s3a://bucket/data/", List.of("a", "b"));
        assertThrows(IllegalArgumentException.class, () -> gen.prefixFor(List.of("only-one")));
    }

    @Test
    void noPartitionColumns() {
        var gen = new PartitionPrefixGenerator("s3a://bucket/table/data/", List.of());
        assertEquals("s3a://bucket/table/data/", gen.prefixFor(List.of()));
    }

    @Test
    void getPartitionColumns() {
        var gen = new PartitionPrefixGenerator("s3a://bucket/table/data/", List.of("event_date", "event_hour"));
        assertEquals(List.of("event_date", "event_hour"), gen.getPartitionColumns());
    }
}
