package io.github.iceberg.cleanup;

import io.github.iceberg.common.UriNormalizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Removes leftover storage objects under an empty table root ({@code data/} + {@code metadata/})
 * after all data files and partitions are gone.
 */
public class EmptyTableCleaner {

    private static final Logger LOG = LoggerFactory.getLogger(EmptyTableCleaner.class);

    private final S3Client s3Client;
    private final DirectoryGuard directoryGuard;
    private final PhysicalDeletionService deletionService;

    public EmptyTableCleaner(S3Client s3Client) {
        this(s3Client, new DirectoryGuard(), new PhysicalDeletionService(s3Client));
    }

    public EmptyTableCleaner(S3Client s3Client, DirectoryGuard directoryGuard, PhysicalDeletionService deletionService) {
        this.s3Client = s3Client;
        this.directoryGuard = directoryGuard;
        this.deletionService = deletionService;
    }

    /**
     * Deletes remaining physical files for a table that passed {@link EmptyTableAnalyzer}.
     *
     * @return deleted object keys (S3 key only, not full URI)
     */
    public List<String> purgeEmptyTableStorage(EmptyTableAnalyzer.Assessment assessment,
                                             Set<String> physicalFiles) {
        if (!assessment.eligibleForTableCleanup()) {
            LOG.debug("Table {} not eligible for storage purge", assessment.tableDataPrefix());
            return List.of();
        }

        Set<String> toDelete = physicalFiles.stream()
                .map(UriNormalizer::normalize)
                .filter(path -> path.startsWith(assessment.tableRootPrefix()))
                .filter(directoryGuard::isAllowed)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        if (toDelete.isEmpty()) {
            LOG.info("No leftover storage objects for empty table at {}", assessment.tableRootPrefix());
            return List.of();
        }

        List<String> deleted = deletionService.deleteOrphans(toDelete);
        LOG.info("Purged {} storage object(s) for empty table at {}",
                deleted.size(), assessment.tableRootPrefix());
        return deleted;
    }

    /**
     * Re-lists {@code data/} and {@code metadata/} under the table root after purge.
     */
    public Set<String> listRemainingObjects(String tableDataPrefix) {
        String dataRoot = UriNormalizer.normalize(tableDataPrefix);
        if (!dataRoot.endsWith("/")) {
            dataRoot += "/";
        }
        String metaRoot = OrphanScanPipeline.metadataPrefix(dataRoot);
        L2PhysicalScanner scanner = new L2PhysicalScanner(s3Client);
        Set<String> remaining = new LinkedHashSet<>(scanner.listFiles(List.of(dataRoot, metaRoot)));
        return remaining.stream()
                .filter(p -> isRealObjectKey(UriNormalizer.extractKey(p)))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static boolean isRealObjectKey(String key) {
        return key != null && !key.isEmpty() && !key.endsWith("/");
    }
}
