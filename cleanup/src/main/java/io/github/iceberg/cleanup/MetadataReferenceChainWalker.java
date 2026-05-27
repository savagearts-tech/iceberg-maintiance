package io.github.iceberg.cleanup;

import io.github.iceberg.common.UriNormalizer;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.FileIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Collects all metadata-directory files reachable from active table snapshots.
 *
 * <p>Uses Iceberg's {@link ReachableFileUtil} for JSON metadata files, manifest lists,
 * and statistics files, and walks each snapshot's manifests via {@link Snapshot#allManifests}.
 * This avoids false-positive metadata orphans for manifest/manifest-list Avro files.
 */
public class MetadataReferenceChainWalker {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataReferenceChainWalker.class);

    private final Table table;
    private final TableOperations ops;

    public MetadataReferenceChainWalker(Table table, TableOperations ops) {
        this.table = table;
        this.ops = ops;
    }

    /**
     * Builds the set of all metadata files referenced by the active table state.
     *
     * @return set of normalized (s3a://) metadata file paths under {@code metadata/}
     */
    public Set<String> buildActiveMetadataSet() {
        Set<String> active = new HashSet<>();
        FileIO io = ops.io();

        addAllNormalized(active, ReachableFileUtil.metadataFileLocations(table, true));
        addAllNormalized(active, ReachableFileUtil.manifestListLocations(table));
        addAllNormalized(active, ReachableFileUtil.statisticsFilesLocations(table));

        for (Snapshot snapshot : table.snapshots()) {
            for (ManifestFile manifest : snapshot.allManifests(io)) {
                addNormalized(active, manifest.path());
            }
        }

        LOG.info("Reachable metadata walk: {} active metadata files for table {}",
                active.size(), table.name());
        return active;
    }

    private static void addAllNormalized(Set<String> target, Iterable<String> paths) {
        for (String path : paths) {
            addNormalized(target, path);
        }
    }

    private static void addNormalized(Set<String> target, String path) {
        if (path != null && !path.isEmpty()) {
            target.add(UriNormalizer.normalize(path));
        }
    }
}
