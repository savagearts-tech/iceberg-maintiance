package io.github.iceberg.cleanup;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DirectoryGuardTest {

    private final DirectoryGuard guard = new DirectoryGuard();

    @Test
    void dataPathAllowed() {
        assertTrue(guard.isAllowed("s3a://bucket/table/data/partition=a/file.parquet"));
    }

    @Test
    void metadataPathAllowed() {
        assertTrue(guard.isAllowed("s3a://bucket/table/metadata/v1.metadata.json"));
    }

    @Test
    void rootLevelPathRejected() {
        assertFalse(guard.isAllowed("s3a://bucket/table/root_file.txt"));
    }

    @Test
    void nullPathRejected() {
        assertFalse(guard.isAllowed(null));
    }

    @Test
    void emptyPathRejected() {
        assertFalse(guard.isAllowed(""));
    }

    @Test
    void arbitraryPathRejected() {
        assertFalse(guard.isAllowed("s3a://bucket/etc/passwd"));
    }

    @Test
    void deepDataPathAllowed() {
        assertTrue(guard.isAllowed("s3a://bucket/table/data/year=2026/month=01/day=15/file.parquet"));
    }

    @Test
    void metadataWithDataPrefixAllowed() {
        // metadata files in a non-standard location under data/ should still be checked
        assertTrue(guard.isAllowed("s3a://bucket/data/metadata/file.avro"));
    }
}
