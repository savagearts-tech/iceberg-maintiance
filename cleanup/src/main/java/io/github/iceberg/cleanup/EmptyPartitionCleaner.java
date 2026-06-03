package io.github.iceberg.cleanup;

import io.github.iceberg.common.UriNormalizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Removes empty Hive-style partition directories under {@code data/} after orphan file deletion.
 *
 * <p>A partition prefix is eligible when:
 * <ul>
 *   <li>It is a partition directory (contains {@code col=val} segments under {@code data/})</li>
 *   <li>No active snapshot references any data file under that prefix</li>
 *   <li>S3 has no remaining data files under the prefix (only optional zero-byte folder markers)</li>
 * </ul>
 */
public class EmptyPartitionCleaner {

    private static final Logger LOG = LoggerFactory.getLogger(EmptyPartitionCleaner.class);

    private final S3Client s3Client;
    private final DirectoryGuard directoryGuard;
    private final PhysicalDeletionService deletionService;

    public EmptyPartitionCleaner(S3Client s3Client) {
        this(s3Client, new DirectoryGuard(), new PhysicalDeletionService(s3Client));
    }

    public EmptyPartitionCleaner(S3Client s3Client, DirectoryGuard directoryGuard, PhysicalDeletionService deletionService) {
        this.s3Client = s3Client;
        this.directoryGuard = directoryGuard;
        this.deletionService = deletionService;
    }

    /**
     * Deletes empty partition directory markers for partitions with no referenced data files.
     *
     * @param tableDataPrefix   table data root, e.g. {@code s3a://bucket/db/tbl/data/}
     * @param referencedFiles   L1-referenced data file paths
     * @param scanPrefixes      prefixes used in L2 scan (partition + metadata)
     * @param deletedFilePaths  data files deleted in the current run (optional)
     * @return partition prefixes that were cleaned (s3a:// paths ending with /)
     */
    public List<String> deleteEmptyPartitions(String tableDataPrefix,
                                              Set<String> referencedFiles,
                                              Collection<String> scanPrefixes,
                                              Collection<String> deletedFilePaths) {
        String dataRoot = UriNormalizer.normalize(tableDataPrefix);
        if (!dataRoot.endsWith("/")) {
            dataRoot += "/";
        }

        Set<String> candidates = collectCandidates(dataRoot, scanPrefixes, deletedFilePaths);
        List<String> cleaned = new java.util.ArrayList<>();

        for (String partitionPrefix : candidates) {
            if (!isDataPartitionPrefix(partitionPrefix, dataRoot)) {
                continue;
            }
            if (!directoryGuard.isAllowed(partitionPrefix)) {
                LOG.warn("DirectoryGuard rejected partition prefix: {}", partitionPrefix);
                continue;
            }
            if (hasReferencedDataFile(partitionPrefix, referencedFiles)) {
                LOG.debug("Partition still referenced in metadata, skipping: {}", partitionPrefix);
                continue;
            }

            List<String> removableKeys = listRemovableKeys(partitionPrefix);
            if (removableKeys.isEmpty()) {
                LOG.debug("Partition prefix already empty on storage: {}", partitionPrefix);
                continue;
            }

            String bucket = L2PhysicalScanner.extractBucket(partitionPrefix);
            List<String> deleted = deletionService.deleteKeysDirect(bucket, removableKeys);
            if (!deleted.isEmpty()) {
                cleaned.add(partitionPrefix);
                LOG.info("Removed {} empty partition marker(s) under {}", deleted.size(), partitionPrefix);
            }
        }

        LOG.info("Empty partition cleanup complete: {} partition(s) cleaned", cleaned.size());
        return cleaned;
    }

    public static Set<String> collectCandidates(String dataRoot,
                                         Collection<String> scanPrefixes,
                                         Collection<String> deletedFilePaths) {
        Set<String> candidates = new LinkedHashSet<>();
        if (scanPrefixes != null) {
            for (String prefix : scanPrefixes) {
                String norm = UriNormalizer.normalize(prefix);
                if (norm.contains("/data/") && !norm.contains("/metadata/")) {
                    candidates.add(ensureTrailingSlash(norm));
                }
            }
        }
        if (deletedFilePaths != null) {
            for (String path : deletedFilePaths) {
                String dir = directoryPrefixForDataFile(path);
                if (dir != null && dir.startsWith(dataRoot)) {
                    candidates.add(dir);
                }
            }
        }
        return candidates;
    }

    public static boolean isDataPartitionPrefix(String prefix, String dataRoot) {
        String norm = ensureTrailingSlash(UriNormalizer.normalize(prefix));
        String root = ensureTrailingSlash(UriNormalizer.normalize(dataRoot));
        if (!norm.startsWith(root) || norm.equals(root)) {
            return false;
        }
        String afterData = norm.substring(norm.indexOf("/data/") + "/data/".length());
        return afterData.contains("=");
    }

    public static boolean hasReferencedDataFile(String partitionPrefix, Set<String> referencedFiles) {
        String prefix = ensureTrailingSlash(UriNormalizer.normalize(partitionPrefix));
        for (String file : referencedFiles) {
            String norm = UriNormalizer.normalize(file);
            if (norm.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Lists S3 keys under the prefix that are safe to remove: folder markers only, no data files.
     */
    private List<String> listRemovableKeys(String partitionPrefix) {
        String bucket = L2PhysicalScanner.extractBucket(partitionPrefix);
        String prefixKey = L2PhysicalScanner.extractKey(ensureTrailingSlash(partitionPrefix));

        List<String> keys = new java.util.ArrayList<>();
        String continuationToken = null;
        do {
            ListObjectsV2Request.Builder req = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(prefixKey);
            if (continuationToken != null) {
                req.continuationToken(continuationToken);
            }
            ListObjectsV2Response resp = s3Client.listObjectsV2(req.build());
            for (S3Object obj : resp.contents()) {
                if (isDataFileKey(obj.key())) {
                    LOG.debug("Partition {} still has data file {}, skipping cleanup",
                            partitionPrefix, obj.key());
                    return List.of();
                }
                if (isFolderMarker(obj)) {
                    keys.add(obj.key());
                }
            }
            continuationToken = resp.nextContinuationToken();
        } while (continuationToken != null);

        return keys;
    }

    private static boolean isDataFileKey(String key) {
        if (key.endsWith("/")) {
            return false;
        }
        return true;
    }

    private static boolean isFolderMarker(S3Object obj) {
        if (obj.key().endsWith("/")) {
            return true;
        }
        return obj.size() != null && obj.size() == 0L;
    }

    static String directoryPrefixForDataFile(String normalizedFilePath) {
        if (normalizedFilePath == null || !normalizedFilePath.contains("/data/")) {
            return null;
        }
        String key = UriNormalizer.extractKey(normalizedFilePath);
        int lastSlash = key.lastIndexOf('/');
        if (lastSlash <= 0) {
            return null;
        }
        String dirKey = key.substring(0, lastSlash + 1);
        String bucket = UriNormalizer.extractBucket(normalizedFilePath);
        return "s3a://" + bucket + "/" + dirKey;
    }

    private static String ensureTrailingSlash(String path) {
        return path.endsWith("/") ? path : path + "/";
    }
}
