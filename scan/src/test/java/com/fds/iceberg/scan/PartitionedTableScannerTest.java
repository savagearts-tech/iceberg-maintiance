package com.fds.iceberg.scan;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PartitionedTableScannerTest {

    @Test
    void parseSingleLevelPartitionPath() {
        assertEquals(List.of("20597"), PartitionedTableScanner.parsePartitionPathValues("event_date_day=20597"));
    }

    @Test
    void parseMultiLevelPartitionPath() {
        assertEquals(List.of("2026", "05", "26"),
                PartitionedTableScanner.parsePartitionPathValues("year=2026/month=05/day=26"));
    }
}
