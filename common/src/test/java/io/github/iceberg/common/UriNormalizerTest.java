package io.github.iceberg.common;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class UriNormalizerTest {

    @Test
    void normalizeS3toS3a() {
        assertEquals("s3a://bucket/key.parquet", UriNormalizer.normalize("s3://bucket/key.parquet"));
    }

    @Test
    void normalizeS3aUnchanged() {
        assertEquals("s3a://bucket/key.parquet", UriNormalizer.normalize("s3a://bucket/key.parquet"));
    }

    @Test
    void normalizeBarePath() {
        assertEquals("s3a://bucket/key.parquet", UriNormalizer.normalize("bucket/key.parquet"));
    }

    @Test
    void normalizeNullReturnsNull() {
        assertNull(UriNormalizer.normalize(null));
    }

    @Test
    void matchSamePaths() {
        assertTrue(UriNormalizer.match("s3://bucket/a.parquet", "s3a://bucket/a.parquet"));
    }

    @Test
    void matchDifferentPaths() {
        assertFalse(UriNormalizer.match("s3://bucket/a.parquet", "s3a://bucket/b.parquet"));
    }

    @Test
    void matchBothNull() {
        assertFalse(UriNormalizer.match(null, null));
    }
}
