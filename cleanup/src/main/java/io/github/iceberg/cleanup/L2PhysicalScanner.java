package io.github.iceberg.cleanup;

import io.github.iceberg.common.UriNormalizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * L2 physical scanner uses S3 {@code ListObjectsV2} with configurable prefixes
 * and pagination to enumerate files on the storage backend.
 *
 * <p>Accepts partition-derived prefixes to narrow the scan scope to specific
 * partition directories. Supports parallel prefix scanning and automatic
 * sub-prefix discovery for efficiency.
 *
 * <p>Maintains an internal cache of {@code lastModified} timestamps from
 * ListObjects responses. Downstream components like {@link CoolingPeriodFilter}
 * can use this cache to avoid redundant HEAD requests.
 */
public class L2PhysicalScanner {

    private static final Logger LOG = LoggerFactory.getLogger(L2PhysicalScanner.class);

    private static final int DEFAULT_PARALLELISM = 32;
    private static final long SCAN_TIMEOUT_MINUTES = 30;

    private final S3Client s3Client;
    private final int parallelism;

    // lastModified cache populated during listFiles(), keyed by normalized s3a:// path
    private final ConcurrentHashMap<String, Instant> lastModifiedCache = new ConcurrentHashMap<>();

    public L2PhysicalScanner(S3Client s3Client) {
        this(s3Client, DEFAULT_PARALLELISM);
    }

    public L2PhysicalScanner(S3Client s3Client, int parallelism) {
        this.s3Client = s3Client;
        this.parallelism = parallelism;
    }

    /**
     * Lists all files under the given S3 prefixes.
     *
     * @param prefixes S3 prefixes to scan (can be partition-level or table-level)
     * @return normalized file paths (s3a://) from all matched prefixes
     */
    public Set<String> listFiles(List<String> prefixes) {
        if (prefixes == null || prefixes.isEmpty()) {
            return Collections.emptySet();
        }
        // Reset cache for fresh scan
        lastModifiedCache.clear();

        String bucket = extractBucket(prefixes.getFirst());
        List<String> relativePrefixes = prefixes.stream()
                .map(L2PhysicalScanner::extractKey)
                .collect(Collectors.toList());

        Queue<String> results = new ConcurrentLinkedQueue<>();

        // 1. Dynamic Sub-Prefix Parallelization (expand if we don't have enough parallel tasks)
        List<String> scanPrefixes = new ArrayList<>(relativePrefixes);
        if (scanPrefixes.size() < parallelism) {
            scanPrefixes = expandPrefixes(bucket, scanPrefixes, results);
        }

        // 2. Parallel Deep Scan
        if (scanPrefixes.isEmpty()) {
            LOG.info("All files were collected during expand phase.");
        } else if (scanPrefixes.size() == 1) {
            LOG.info("Starting SINGLE-THREADED scan for 1 prefix...");
            listPrefix(bucket, scanPrefixes.getFirst(), results);
        } else {
            int activeThreads = Math.min(parallelism, scanPrefixes.size());
            LOG.info("Starting PARALLEL scan for {} prefixes using Virtual Threads...", scanPrefixes.size());
            ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
            try {
                List<Future<?>> futures = new ArrayList<>();
                for (String prefix : scanPrefixes) {
                    futures.add(executor.submit(() -> listPrefix(bucket, prefix, results)));
                }
                // Wait for all tasks and collect any errors
                for (Future<?> f : futures) {
                    try {
                        f.get();
                    } catch (ExecutionException e) {
                        LOG.error("Parallel prefix scan failed", e.getCause());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        LOG.warn("Prefix scan was interrupted");
                        break;
                    }
                }
            } finally {
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(SCAN_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
                        executor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    executor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
        }

        LOG.info("L2 physical scan complete: {} files from {} initial prefixes", results.size(), relativePrefixes.size());
        return results.stream()
                .map(p -> "s3a://" + bucket + "/" + p)
                .collect(Collectors.toSet());
    }

    /**
     * Attempts to dynamically discover subdirectories for better parallelization.
     */
    private List<String> expandPrefixes(String bucket, List<String> initialPrefixes, Queue<String> directResults) {
        List<String> expanded = new ArrayList<>();
        LOG.info("Expanding {} prefixes to improve parallelism...", initialPrefixes.size());

        for (String prefix : initialPrefixes) {
            String token = null;
            boolean hasSubDirs = false;
            boolean firstPage = true;

            do {
                ListObjectsV2Request.Builder reqBuilder = ListObjectsV2Request.builder()
                        .bucket(bucket).prefix(prefix).delimiter("/");
                if (token != null) reqBuilder.continuationToken(token);

                ListObjectsV2Response resp = s3Client.listObjectsV2(reqBuilder.build());

                if (firstPage) {
                    hasSubDirs = resp.hasCommonPrefixes() && !resp.commonPrefixes().isEmpty();
                    firstPage = false;
                }

                if (hasSubDirs) {
                    // Collect files at this level (we don't want to scan this parent prefix again)
                    for (S3Object obj : resp.contents()) {
                        if (!obj.key().endsWith("/")) { // skip directory markers
                            directResults.add(obj.key());
                            lastModifiedCache.put("s3a://" + bucket + "/" + obj.key(), obj.lastModified());
                        }
                    }
                    // Collect subdirectories to be deep-scanned later
                    if (resp.hasCommonPrefixes()) {
                        for (CommonPrefix cp : resp.commonPrefixes()) {
                            expanded.add(cp.prefix());
                        }
                    }
                    token = resp.nextContinuationToken();
                } else {
                    // No subdirectories found on the first page -> it's a flat directory.
                    // Stop expanding this prefix to avoid sequentially paginating millions of files.
                    expanded.add(prefix);
                    token = null;
                }
            } while (token != null);
        }

        LOG.info("Expanded into {} sub-prefixes for parallel deep scanning.", expanded.size());
        return expanded;
    }

    /**
     * Returns the last modified timestamp for a path (from the most recent scan cache).
     * Returns {@code null} if the path was not listed in the last scan.
     */
    public Instant getLastModified(String normalizedPath) {
        return lastModifiedCache.get(normalizedPath);
    }

    /**
     * Returns an unmodifiable view of the lastModified cache.
     * Downstream components (e.g. {@link PhysicalDeletionService}) use this to
     * avoid redundant HEAD requests during cooling period checks.
     */
    public Map<String, Instant> getLastModifiedCache() {
        return Collections.unmodifiableMap(lastModifiedCache);
    }

    private void listPrefix(String bucket, String prefix, Queue<String> results) {
        String continuationToken = null;
        int pageCount = 0;
        do {
            ListObjectsV2Request.Builder reqBuilder = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(prefix);
            if (continuationToken != null) {
                reqBuilder.continuationToken(continuationToken);
            }
            ListObjectsV2Response resp = s3Client.listObjectsV2(reqBuilder.build());
            for (S3Object obj : resp.contents()) {
                String s3aPath = "s3a://" + bucket + "/" + obj.key();
                results.add(obj.key());
                lastModifiedCache.put(s3aPath, obj.lastModified());
            }
            continuationToken = resp.nextContinuationToken();
            pageCount++;
        } while (continuationToken != null);
        LOG.debug("Listed {} pages for prefix '{}'", pageCount, prefix);
    }

    static String extractBucket(String s3Uri) {
        return UriNormalizer.extractBucket(s3Uri);
    }

    static String extractKey(String s3Uri) {
        return UriNormalizer.extractKey(s3Uri);
    }
}
