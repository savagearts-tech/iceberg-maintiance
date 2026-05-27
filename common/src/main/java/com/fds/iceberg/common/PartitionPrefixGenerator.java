package com.fds.iceberg.common;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts partition value tuples to S3 prefix strings for directed physical scanning.
 *
 * <p>Partition structure is auto-detected from the Iceberg table metadata.
 * Supports single-level ({@code data/col=val/}) and multi-level
 * ({@code data/year=2026/month=05/day=26/}) partition layouts.
 */
public class PartitionPrefixGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionPrefixGenerator.class);

    private final List<String> partitionColumns;
    private final String tableDataPrefix;

    /**
     * @param tableDataPrefix the S3 prefix for the table's data directory, e.g. {@code s3a://bucket/table/data/}
     */
    public PartitionPrefixGenerator(String tableDataPrefix, Table table) {
        this.tableDataPrefix = normalizePrefix(tableDataPrefix);
        this.partitionColumns = extractPartitionColumns(table);
        LOG.debug("Partition columns detected: {}; data prefix: {}", this.partitionColumns, this.tableDataPrefix);
    }

    /**
     * Creates a spec for testing with explicit partition columns.
     */
    PartitionPrefixGenerator(String tableDataPrefix, List<String> partitionColumns) {
        this.tableDataPrefix = normalizePrefix(tableDataPrefix);
        this.partitionColumns = partitionColumns;
    }

    /**
     * Generates an S3 prefix for a single partition value tuple.
     *
     * @param partitionValues ordered partition values matching the table's partition spec
     * @return the S3 prefix, e.g. {@code s3a://bucket/table/data/event_date=2026-05-01/}
     */
    public String prefixFor(List<String> partitionValues) {
        if (partitionValues.size() != partitionColumns.size()) {
            throw new IllegalArgumentException(
                    "Expected " + partitionColumns.size() + " partition values, got " + partitionValues.size());
        }
        StringBuilder sb = new StringBuilder(tableDataPrefix);
        for (int i = 0; i < partitionColumns.size(); i++) {
            sb.append(partitionColumns.get(i)).append("=").append(partitionValues.get(i)).append("/");
        }
        return sb.toString();
    }

    /**
     * Generates S3 prefixes for multiple partition value tuples.
     */
    public List<String> prefixesFor(List<List<String>> partitionValuesList) {
        return partitionValuesList.stream()
                .map(this::prefixFor)
                .collect(Collectors.toList());
    }

    /**
     * Builds an S3 prefix directly from a {@link org.apache.iceberg.PartitionSpec#partitionToPath} value.
     */
    public String prefixFromPartitionPath(String partitionPath) {
        if (partitionPath == null || partitionPath.isBlank()) {
            return tableDataPrefix;
        }
        String relative = partitionPath.startsWith("/") ? partitionPath.substring(1) : partitionPath;
        if (!relative.endsWith("/")) {
            relative += "/";
        }
        return tableDataPrefix + relative;
    }

    /**
     * Generates S3 prefixes for partition paths returned by {@code partitionToPath()}.
     */
    public List<String> prefixesFromPartitionPaths(Collection<String> partitionPaths) {
        return partitionPaths.stream()
                .map(this::prefixFromPartitionPath)
                .distinct()
                .collect(Collectors.toList());
    }

    /**
     * Returns a single "greedy" prefix covering all partitions under this table's data directory.
     * Used when no partition filter is applied (fallback to full scan).
     */
    public String fullPrefix() {
        return tableDataPrefix;
    }

    public List<String> getPartitionColumns() {
        return partitionColumns;
    }

    private static String normalizePrefix(String prefix) {
        if (prefix == null) return "";
        String n = prefix.trim();
        if (!n.endsWith("/")) {
            n += "/";
        }
        // Ensure it uses s3a:// for consistency
        return UriNormalizer.normalize(n);
    }

    private static List<String> extractPartitionColumns(Table table) {
        // Merge partition field names from ALL partition specs (tables can be
        // repartitioned over time — old data uses old specs with different columns).
        // Using all specs ensures prefix generation covers both old and new layouts.
        return table.specs().values().stream()
                .flatMap(spec -> spec.fields().stream())
                .map(PartitionField::name)
                .distinct()
                .collect(Collectors.toList());
    }
}
