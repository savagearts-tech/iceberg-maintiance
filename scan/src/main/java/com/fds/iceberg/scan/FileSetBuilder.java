package com.fds.iceberg.scan;

import com.fds.iceberg.common.UriNormalizer;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.Table;
import org.apache.iceberg.FileScanTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Builds an in-memory set of all data file paths currently referenced by active snapshots.
 *
 * <p>Uses {@link Table#newScan()} combined with {@code planFiles()} to enumerate
 * all actively referenced data files. For best performance, use with
 * {@link PartitionedTableScanner} which caches the scan result across callers.
 */
public class FileSetBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(FileSetBuilder.class);

    private final Table table;

    public FileSetBuilder(Table table) {
        this.table = table;
    }

    /**
     * Collects all data file paths referenced by the table's active snapshots,
     * normalized via {@link UriNormalizer#normalize(String)}.
     */
    public Set<String> buildReferencedFileSet() {
        Set<String> files = new HashSet<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                String path = task.file().path().toString();
                files.add(UriNormalizer.normalize(path));
            }
        } catch (Exception e) {
            LOG.warn("Failed to scan table files", e);
        }

        LOG.info("Built referenced file set: {} files from metadata scan", files.size());
        return files;
    }
}
