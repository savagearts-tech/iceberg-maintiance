package io.github.iceberg.cleanup;

import io.github.iceberg.common.UriNormalizer;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Determines whether an Iceberg table has no remaining data or partitions and
 * should be considered for table-level storage or catalog cleanup.
 *
 * <p>Table cleanup is stricter than partition cleanup: every partition must first
 * satisfy {@link EmptyPartitionCleaner} eligibility and be absent from storage
 * before the table itself is eligible.
 */
public final class EmptyTableAnalyzer {

    private EmptyTableAnalyzer() {}

    public record Assessment(
            String tableDataPrefix,
            boolean hasReferencedData,
            int physicalDataFileCount,
            int physicalMetadataFileCount,
            int remainingPartitionCount,
            int blockingPartitionCount,
            List<String> remainingPartitionPrefixes,
            boolean eligibleForTableCleanup
    ) {
        public String tableRootPrefix() {
            return EmptyTableAnalyzer.tableRootPrefix(tableDataPrefix);
        }

        /** Partitions that still exist on storage but fail partition-delete preconditions. */
        public boolean hasBlockingPartitions() {
            return blockingPartitionCount > 0;
        }

        /** Partitions deletable by {@link EmptyPartitionCleaner} but not yet removed from storage. */
        public boolean hasUncleanedPartitions() {
            return remainingPartitionCount > blockingPartitionCount;
        }
    }

    /**
     * @param tableDataPrefix  table {@code data/} root
     * @param referencedFiles  L1-referenced data file paths
     * @param physicalFiles    paths from L2 scan (data + metadata); only source for partition presence
     */
    public static Assessment analyze(String tableDataPrefix,
                                   Set<String> referencedFiles,
                                   Set<String> physicalFiles) {
        String dataRoot = ensureTrailingSlash(UriNormalizer.normalize(tableDataPrefix));
        int dataFiles = countRealDataFiles(dataRoot, physicalFiles);
        int metaFiles = countMetadataFiles(physicalFiles);
        boolean hasRefs = referencedFiles != null && !referencedFiles.isEmpty();

        Set<String> remainingPartitions = discoverRemainingPartitions(dataRoot, physicalFiles);
        int blocking = 0;
        for (String partition : remainingPartitions) {
            if (!isPartitionDeletable(partition, referencedFiles, physicalFiles)) {
                blocking++;
            }
        }

        boolean eligible = !hasRefs
                && dataFiles == 0
                && remainingPartitions.isEmpty();

        return new Assessment(
                dataRoot,
                hasRefs,
                dataFiles,
                metaFiles,
                remainingPartitions.size(),
                blocking,
                List.copyOf(remainingPartitions),
                eligible);
    }

    /**
     * A partition is deletable when it satisfies the same rules as {@link EmptyPartitionCleaner}:
     * no referenced data files and no physical data files under the prefix.
     */
    public static boolean isPartitionDeletable(String partitionPrefix,
                                               Set<String> referencedFiles,
                                               Set<String> physicalFiles) {
        return !EmptyPartitionCleaner.hasReferencedDataFile(partitionPrefix, referencedFiles)
                && !hasPhysicalDataFilesUnder(partitionPrefix, physicalFiles);
    }

    /**
     * Discovers Hive-style partition prefixes still present on storage under {@code data/}.
     * Uses only L2 physical paths (not metadata-derived scan prefixes).
     */
    public static Set<String> discoverRemainingPartitions(String dataRoot, Set<String> physicalFiles) {
        Set<String> partitions = new LinkedHashSet<>();
        String rootUri = ensureTrailingSlash(UriNormalizer.normalize(dataRoot));
        String rootKey = UriNormalizer.extractKey(rootUri);
        if (!rootKey.endsWith("/")) {
            rootKey += "/";
        }

        if (physicalFiles == null) {
            return partitions;
        }
        for (String path : physicalFiles) {
            String norm = UriNormalizer.normalize(path);
            if (!norm.startsWith(rootUri)) {
                continue;
            }
            String partition = extractPartitionPrefix(norm, rootUri, rootKey);
            if (partition != null) {
                partitions.add(partition);
            }
        }
        return partitions;
    }

    static String extractPartitionPrefix(String normalizedPath, String dataRootUri, String dataRootKey) {
        String key = UriNormalizer.extractKey(normalizedPath);
        if (!key.startsWith(dataRootKey)) {
            return null;
        }
        String relative = key.substring(dataRootKey.length());
        if (!relative.contains("=")) {
            return null;
        }
        int lastSlash = relative.lastIndexOf('/');
        String dirRelative = lastSlash >= 0 ? relative.substring(0, lastSlash + 1) : relative;
        if (!dirRelative.contains("=")) {
            return null;
        }
        String bucket = UriNormalizer.extractBucket(normalizedPath);
        return "s3a://" + bucket + "/" + dataRootKey + dirRelative;
    }

    static boolean hasPhysicalDataFilesUnder(String partitionPrefix, Set<String> physicalFiles) {
        if (physicalFiles == null || physicalFiles.isEmpty()) {
            return false;
        }
        String prefix = ensureTrailingSlash(UriNormalizer.normalize(partitionPrefix));
        for (String path : physicalFiles) {
            String norm = UriNormalizer.normalize(path);
            if (norm.startsWith(prefix) && isRealObjectKey(UriNormalizer.extractKey(norm))) {
                return true;
            }
        }
        return false;
    }

    public static String tableRootPrefix(String tableDataPrefix) {
        String norm = ensureTrailingSlash(UriNormalizer.normalize(tableDataPrefix));
        if (norm.endsWith("/data/")) {
            return norm.substring(0, norm.length() - "data/".length());
        }
        int idx = norm.lastIndexOf("/data/");
        if (idx >= 0) {
            return norm.substring(0, idx + 1);
        }
        return norm;
    }

    static int countRealDataFiles(String dataRoot, Set<String> physicalFiles) {
        if (physicalFiles == null || physicalFiles.isEmpty()) {
            return 0;
        }
        String root = ensureTrailingSlash(dataRoot);
        int count = 0;
        for (String path : physicalFiles) {
            String norm = UriNormalizer.normalize(path);
            if (!norm.contains("/data/") || !norm.startsWith(root)) {
                continue;
            }
            String key = UriNormalizer.extractKey(norm);
            if (isRealObjectKey(key)) {
                count++;
            }
        }
        return count;
    }

    static int countMetadataFiles(Set<String> physicalFiles) {
        if (physicalFiles == null || physicalFiles.isEmpty()) {
            return 0;
        }
        int count = 0;
        for (String path : physicalFiles) {
            String norm = UriNormalizer.normalize(path);
            if (!norm.contains("/metadata/")) {
                continue;
            }
            String key = UriNormalizer.extractKey(norm);
            if (isRealObjectKey(key)) {
                count++;
            }
        }
        return count;
    }

    private static boolean isRealObjectKey(String key) {
        return key != null && !key.isEmpty() && !key.endsWith("/");
    }

    private static String ensureTrailingSlash(String path) {
        return path.endsWith("/") ? path : path + "/";
    }
}
