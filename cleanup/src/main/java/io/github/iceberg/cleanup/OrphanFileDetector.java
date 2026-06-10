package io.github.iceberg.cleanup;

import io.github.iceberg.common.UriNormalizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Detects orphan (zombie) files by comparing physical files against
 * metadata-referenced files.
 *
 * <p>For {@code data/} files: uses Set difference (physical - referenced).
 * For {@code metadata/} files: uses reference chain result from
 * {@link MetadataReferenceChainWalker}.
 */
public class OrphanFileDetector {

    private static final Logger LOG = LoggerFactory.getLogger(OrphanFileDetector.class);

    private static final String DATA_PREFIX = "/data/";
    private static final String METADATA_PREFIX = "/metadata/";

    /**
     * Identifies orphan data files via set difference.
     *
     * @param physicalFiles   all files found on the storage backend
     * @param referencedFiles files referenced by active Iceberg snapshots
     * @return orphan file paths that exist in storage but not in metadata
     */
    public Set<String> detectDataOrphans(Set<String> physicalFiles, Set<String> referencedFiles) {
        if (referencedFiles == null) {
            throw new IllegalArgumentException("referencedFiles must not be null — null would treat all files as orphans");
        }
        Set<String> orphans = physicalFiles.stream()
                .filter(p -> p.contains(DATA_PREFIX))
                .map(UriNormalizer::normalize)
                .filter(p -> !referencedFiles.contains(p))
                .collect(Collectors.toSet());

        LOG.info("Data orphan detection: {} orphans from {} physical files",
                orphans.size(), physicalFiles.size());
        return orphans;
    }

    /**
     * Identifies orphan metadata files by comparing physical files against the
     * active metadata reference chain.
     *
     * @param physicalFiles   all files found on the storage backend
     * @param activeMetadata  files from {@link MetadataReferenceChainWalker}
     * @return orphan metadata file paths
     */
    public Set<String> detectMetadataOrphans(Set<String> physicalFiles, Set<String> activeMetadata) {
        Set<String> normalizedActive = activeMetadata.stream()
                .map(UriNormalizer::normalize)
                .collect(Collectors.toSet());

        Set<String> orphans = physicalFiles.stream()
                .filter(p -> p.contains(METADATA_PREFIX))
                .map(UriNormalizer::normalize)
                .filter(p -> !normalizedActive.contains(p))
                .collect(Collectors.toSet());

        LOG.info("Metadata orphan detection: {} orphans from {} physical files ({} active in chain)",
                orphans.size(), physicalFiles.size(), normalizedActive.size());
        return orphans;
    }
}
