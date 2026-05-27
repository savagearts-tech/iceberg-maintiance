package io.github.iceberg.cleanup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates that candidate file paths are within allowed directories.
 * Root-level files are rejected. Both {@code /data/} and {@code /metadata/}
 * directories are allowed.
 */
public class DirectoryGuard {

    private static final Logger LOG = LoggerFactory.getLogger(DirectoryGuard.class);

    private static final String DATA_PREFIX = "/data/";
    private static final String METADATA_PREFIX = "/metadata/";

    /**
     * Returns {@code true} if the file path is in an allowed directory.
     */
    public boolean isAllowed(String normalizedPath) {
        if (normalizedPath == null || normalizedPath.isBlank()) {
            return false;
        }
        String path = normalizedPath.trim();

        // root-level files are always rejected
        int bucketEnd = path.indexOf("/", path.indexOf("://") + 3);
        if (bucketEnd < 0) {
            return false;
        }
        String afterBucket = path.substring(bucketEnd + 1);
        // after bucket, we expect data/ or metadata/ as the first directory component
        if (afterBucket.startsWith(DATA_PREFIX.substring(1)) || afterBucket.startsWith(METADATA_PREFIX.substring(1))) {
            return true;
        }
        // check for multi-level: e.g. table_root/data/...
        if (afterBucket.contains(DATA_PREFIX) || afterBucket.contains(METADATA_PREFIX)) {
            return true;
        }

        LOG.warn("DirectoryGuard rejected path outside data/ and metadata/: {}", path);
        return false;
    }
}
