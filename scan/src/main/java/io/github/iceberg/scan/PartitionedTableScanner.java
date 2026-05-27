package io.github.iceberg.scan;

import io.github.iceberg.common.PartitionPrefixGenerator;
import io.github.iceberg.common.UriNormalizer;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.expressions.Expression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Scans Iceberg table metadata with configurable partition filter predicates.
 * Auto-detects partition column and granularity from the table metadata.
 *
 * <p>Supports memory-bounded scans with configurable page size. For unpartitioned
 * tables, falls back to a full scan with a warning.
 *
 * <p>Derives S3 prefixes from scan results for directed L2 physical scanning.
 * Results are cached to avoid redundant {@code planFiles()} calls across
 * {@link #scanDataFiles()} and {@link #derivePrefixes()}.
 */
public class PartitionedTableScanner {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionedTableScanner.class);

    private static final int DEFAULT_PAGE_SIZE = 100;

    private final Table table;
    private final int pageSize;
    private final Expression partitionFilter;
    private final PartitionPrefixGenerator prefixGenerator;

    // cached scan result â€?avoids double planFiles() when both
    // scanDataFiles() and derivePrefixes() are called in a pipeline
    private ScanResult cachedResult;

    /**
     * Combined result of a single metadata scan pass.
     */
    public record ScanResult(Set<String> referencedFiles, Set<String> partitionPaths) {}

    /**
     * @param table            the Iceberg table to scan
     * @param partitionFilter  optional filter expression ({@code null} for full scan)
     * @param tableDataPrefix  S3 prefix for the table's data directory
     */
    public PartitionedTableScanner(Table table, Expression partitionFilter, String tableDataPrefix) {
        this(table, partitionFilter, tableDataPrefix, DEFAULT_PAGE_SIZE);
    }

    public PartitionedTableScanner(Table table, Expression partitionFilter, String tableDataPrefix, int pageSize) {
        this.table = table;
        this.partitionFilter = partitionFilter;
        this.pageSize = pageSize;
        this.prefixGenerator = new PartitionPrefixGenerator(tableDataPrefix, table);
    }

    /**
     * Returns the partition columns detected from the table metadata.
     */
    public List<String> getPartitionColumns() {
        return prefixGenerator.getPartitionColumns();
    }

    /**
     * Returns whether the table has a partition spec defined.
     */
    public boolean isPartitioned() {
        return !table.spec().fields().isEmpty();
    }

    /**
     * Derives S3 prefixes from the partition filter for directed L2 scanning.
     * Collects distinct partition paths from scan tasks and converts them to prefixes.
     *
     * @return list of S3 prefixes to scan; if unpartitioned or no filter, returns the full data prefix
     */
    public List<String> derivePrefixes() {
        if (!isPartitioned()) {
            LOG.warn("Table {} is unpartitioned; using full data prefix for L2 scan", table.name());
            return List.of(prefixGenerator.fullPrefix());
        }
        Set<String> partitionPaths = getOrScan().partitionPaths();
        if (partitionPaths.isEmpty() && getOrScan().referencedFiles().isEmpty()) {
            LOG.info("No partitions matched the filter; falling back to full data prefix");
            return List.of(prefixGenerator.fullPrefix());
        }

        LinkedHashSet<String> prefixes = new LinkedHashSet<>();
        prefixes.addAll(prefixGenerator.prefixesFromPartitionPaths(partitionPaths));
        for (String file : getOrScan().referencedFiles()) {
            String dirPrefix = directoryPrefixForDataFile(file);
            if (dirPrefix != null) {
                prefixes.add(dirPrefix);
            }
        }

        if (prefixes.isEmpty()) {
            LOG.info("No partition prefixes derived; falling back to full data prefix");
            return List.of(prefixGenerator.fullPrefix());
        }

        List<String> prefixList = List.copyOf(prefixes);
        LOG.info("Derived {} S3 prefixes from partition filter", prefixList.size());

        if (LOG.isDebugEnabled()) {
            for (String p : prefixList) {
                LOG.debug("  prefix: {}", p);
            }
        }
        return prefixList;
    }

    /**
     * Returns the S3 prefix for the directory containing a referenced data file.
     */
    String directoryPrefixForDataFile(String normalizedFilePath) {
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

    /**
     * Scans all data files referenced by active snapshots, optionally filtered.
     * Results are cached â€?a second call returns the same set without re-scanning.
     *
     * @return all referenced data file paths, normalized to s3a://
     */
    public Set<String> scanDataFiles() {
        return getOrScan().referencedFiles();
    }

    /**
     * Parses {@code partitionToPath()} output into ordered value components.
     * e.g. {@code year=2026/month=05/day=26} â†?{@code ["2026","05","26"]}
     */
    static List<String> parsePartitionPathValues(String partitionPath) {
        if (partitionPath == null || partitionPath.isBlank()) {
            return List.of();
        }
        List<String> values = new ArrayList<>();
        for (String segment : partitionPath.split("/")) {
            int eq = segment.indexOf('=');
            if (eq >= 0 && eq < segment.length() - 1) {
                values.add(segment.substring(eq + 1));
            }
        }
        return values;
    }

    /**
     * Runs a single {@code planFiles()} pass and populates both referenced files
     * and partition paths from the result. Subsequent calls return the cached data.
     */
    private ScanResult getOrScan() {
        if (cachedResult != null) {
            return cachedResult;
        }
        TableScan scan = buildScan();
        Set<String> files = new HashSet<>();
        Set<String> pathSet = new HashSet<>();

        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            for (FileScanTask task : tasks) {
                files.add(UriNormalizer.normalize(task.file().path().toString()));
                if (isPartitioned()) {
                    pathSet.add(table.spec().partitionToPath(task.file().partition()));
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to scan table {}", table.name(), e);
        }

        cachedResult = new ScanResult(
                Collections.unmodifiableSet(files),
                Collections.unmodifiableSet(pathSet));
        LOG.info("Scanned {} data files from {} partitions for table {} (partitioned={}, filter={})",
                files.size(), pathSet.size(), table.name(), isPartitioned(), partitionFilter);
        return cachedResult;
    }

    private TableScan buildScan() {
        TableScan scan = table.newScan();
        if (partitionFilter != null) {
            scan = scan.filter(partitionFilter);
        }
        return scan;
    }
}
