package io.github.iceberg.cleanup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Bulk deletes orphan files from S3 with per-file error handling.
 *
 * <p>Supports batched deletion and partial failure handling (S3's
 * {@code DeleteObjects} supports up to 1000 keys per request).
 *
 * <p>Passes the L2 scanner's {@code lastModified} cache to
 * {@link CoolingPeriodFilter} to avoid redundant HEAD requests.
 */
public class PhysicalDeletionService {

    private static final Logger LOG = LoggerFactory.getLogger(PhysicalDeletionService.class);

    private static final int BATCH_SIZE = 1000;

    private final S3Client s3Client;
    private final CoolingPeriodFilter coolingFilter;
    private final DirectoryGuard directoryGuard;
    private Map<String, Instant> lastModifiedCache;

    public PhysicalDeletionService(S3Client s3Client) {
        this(s3Client, new CoolingPeriodFilter(), new DirectoryGuard());
    }

    public PhysicalDeletionService(S3Client s3Client, CoolingPeriodFilter coolingFilter, DirectoryGuard directoryGuard) {
        this.s3Client = s3Client;
        this.coolingFilter = coolingFilter;
        this.directoryGuard = directoryGuard;
    }

    /**
     * Attach a lastModified cache (from L2 scanner) to avoid HEAD requests
     * during cooling period checks.
     */
    public PhysicalDeletionService withLastModifiedCache(Map<String, Instant> cache) {
        this.lastModifiedCache = cache;
        return this;
    }

    /**
     * Deletes the given orphan files after passing them through safety filters.
     *
     * @param orphanFiles set of s3a:// paths to delete
     * @return list of successful deletions
     */
    public List<String> deleteOrphans(Set<String> orphanFiles) {
        // 1. Directory guard
        Set<String> guarded = orphanFiles.stream()
                .filter(path -> {
                    boolean allowed = directoryGuard.isAllowed(path);
                    if (!allowed) {
                        LOG.warn("DirectoryGuard rejected: {}", path);
                    }
                    return allowed;
                })
                .collect(Collectors.toSet());

        // 2. Cooling period filter (uses L2 cache when available)
        Set<String> cooled = guarded.stream()
                .filter(path -> {
                    boolean eligible = coolingFilter.isEligible(path, s3Client, lastModifiedCache);
                    if (!eligible) {
                        LOG.debug("CoolingPeriodFilter protected: {}", path);
                    }
                    return eligible;
                })
                .collect(Collectors.toSet());

        // 3. Batch delete
        String bucket = extractBucket(cooled);
        List<String> batch = new ArrayList<>();
        List<String> deleted = new ArrayList<>();
        List<String> failed = new ArrayList<>();

        for (String path : cooled) {
            String key = extractKey(path);
            batch.add(key);
            if (batch.size() >= BATCH_SIZE) {
                deleteBatch(bucket, batch, deleted, failed);
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            deleteBatch(bucket, batch, deleted, failed);
        }

        LOG.info("Physical deletion complete: {} deleted, {} failed", deleted.size(), failed.size());
        return deleted;
    }

    /**
     * Deletes keys without cooling or directory-guard filters. Caller must enforce safety.
     */
    public List<String> deleteKeysDirect(String bucket, List<String> keys) {
        if (keys == null || keys.isEmpty()) {
            return List.of();
        }
        List<String> deleted = new ArrayList<>();
        List<String> failed = new ArrayList<>();
        List<String> batch = new ArrayList<>();
        for (String key : keys) {
            batch.add(key);
            if (batch.size() >= BATCH_SIZE) {
                deleteBatch(bucket, batch, deleted, failed);
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            deleteBatch(bucket, batch, deleted, failed);
        }
        return deleted;
    }

    private void deleteBatch(String bucket, List<String> keys, List<String> deleted, List<String> failed) {
        List<ObjectIdentifier> objects = keys.stream()
                .map(k -> ObjectIdentifier.builder().key(k).build())
                .collect(Collectors.toList());

        DeleteObjectsRequest request = DeleteObjectsRequest.builder()
                .bucket(bucket)
                .delete(Delete.builder().objects(objects).build())
                .build();

        try {
            DeleteObjectsResponse response = s3Client.deleteObjects(request);
            if (response.hasDeleted()) {
                response.deleted().forEach(obj -> deleted.add(obj.key()));
            }
            if (response.hasErrors()) {
                response.errors().forEach(err -> {
                    LOG.error("Failed to delete {}: {} - {}", err.key(), err.code(), err.message());
                    failed.add(err.key());
                });
            }
        } catch (Exception e) {
            LOG.error("Batch delete failed for {} keys", keys.size(), e);
            failed.addAll(keys);
        }
    }

    private static String extractBucket(Set<String> paths) {
        for (String p : paths) {
            return L2PhysicalScanner.extractBucket(p);
        }
        return "";
    }

    private static String extractKey(String path) {
        return L2PhysicalScanner.extractKey(path);
    }
}
