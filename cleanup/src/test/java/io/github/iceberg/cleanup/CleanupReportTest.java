package io.github.iceberg.cleanup;

import org.junit.jupiter.api.Test;
import java.util.List;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

class CleanupReportTest {

    @Test
    void summary() {
        var report = new CleanupReport(
                Instant.parse("2026-05-26T10:00:00Z"),
                "test_table",
                List.of(1L, 2L),
                List.of("data/file1.parquet"),
                List.of("metadata/v1.json"),
                1024L,
                List.of());
        String summary = report.summary();
        assertTrue(summary.contains("test_table"));
        assertTrue(summary.contains("2"));
        assertTrue(summary.contains("1024"));
    }

    @Test
    void emptyReport() {
        var report = new CleanupReport(
                Instant.now(),
                "empty_table",
                List.of(),
                List.of(),
                List.of(),
                0L,
                List.of());
        assertDoesNotThrow(report::summary);
    }
}
