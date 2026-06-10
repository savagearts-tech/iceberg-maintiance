package io.github.iceberg.cli;

import io.github.iceberg.cleanup.*;
import io.github.iceberg.common.RetentionConfig;
import io.github.iceberg.common.UriNormalizer;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Command to execute physical deletion of orphan files with all safety checks.
 */
public class CleanupCommand {

    private static final Logger LOG = LoggerFactory.getLogger(CleanupCommand.class);

    private final Table table;
    private final TableOperations tableOperations;
    private final String tableDataPrefix;
    private final RetentionConfig retentionConfig;
    private final S3Client s3Client;
    private final int coolingDays;
    private final boolean dryRun;
    private final Expression partitionFilter;
    private final boolean purgeEmptyTables;
    private final boolean dropCatalog;
    private final JdbcCatalog catalog;
    private final TableIdentifier tableId;
    private final java.util.List<String> explicitDataPrefixes;
    private final boolean metadataOnly;

    public CleanupCommand(Table table, TableOperations tableOperations, String tableDataPrefix,
                          RetentionConfig retentionConfig, S3Client s3Client, int coolingDays, boolean dryRun) {
        this(table, tableOperations, tableDataPrefix, retentionConfig, s3Client, coolingDays, dryRun, null,
                false, false, null, null, null, false);
    }

    public CleanupCommand(Table table, TableOperations tableOperations, String tableDataPrefix,
                          RetentionConfig retentionConfig, S3Client s3Client, int coolingDays,
                          boolean dryRun, Expression partitionFilter) {
        this(table, tableOperations, tableDataPrefix, retentionConfig, s3Client, coolingDays, dryRun, partitionFilter,
                false, false, null, null, null, false);
    }

    public CleanupCommand(Table table, TableOperations tableOperations, String tableDataPrefix,
                          RetentionConfig retentionConfig, S3Client s3Client, int coolingDays,
                          boolean dryRun, Expression partitionFilter,
                          boolean purgeEmptyTables, boolean dropCatalog,
                          JdbcCatalog catalog, TableIdentifier tableId, java.util.List<String> explicitDataPrefixes,
                          boolean metadataOnly) {
        this.table = table;
        this.tableOperations = tableOperations;
        this.tableDataPrefix = tableDataPrefix;
        this.retentionConfig = retentionConfig;
        this.s3Client = s3Client;
        this.coolingDays = coolingDays;
        this.dryRun = dryRun;
        this.partitionFilter = partitionFilter;
        this.purgeEmptyTables = purgeEmptyTables;
        this.dropCatalog = dropCatalog;
        this.catalog = catalog;
        this.tableId = tableId;
        this.explicitDataPrefixes = explicitDataPrefixes;
        this.metadataOnly = metadataOnly;
    }

    public void execute() {
        // Phase 1: Expire snapshots (dual retention)
        SnapshotExpiryService expiryService = new SnapshotExpiryService(table, retentionConfig);
        List<Long> expiredIds = expiryService.expireSnapshots(dryRun);
        System.out.println(dryRun
                ? "Dry-run: " + expiredIds.size() + " snapshots would be expired"
                : "Expired " + expiredIds.size() + " snapshots");

        // Phase 2: Single-pass orphan scan (L1 + L2)
        OrphanScanPipeline.Result scan = new OrphanScanPipeline().execute(
                table, tableOperations, tableDataPrefix, s3Client, partitionFilter, explicitDataPrefixes, metadataOnly);

        System.out.println("L1 scan: " + (scan.referencedFiles() != null ? scan.referencedFiles().size() : 0) + " files referenced in metadata");
        System.out.println("L2 scan: " + scan.physicalFiles().size() + " files on storage");
        System.out.println("Data orphans: " + scan.dataOrphans().size());
        System.out.println("Metadata orphans: " + scan.metadataOrphans().size());

        Set<String> dataOrphans = scan.dataOrphans();
        Set<String> metadataOrphans = scan.metadataOrphans();

        if (dryRun) {
            if (!dataOrphans.isEmpty()) {
                System.out.println("Dry-run: would delete " + dataOrphans.size() + " orphan data files:");
                dataOrphans.forEach(p -> System.out.println("  " + p));
            }
            if (!metadataOrphans.isEmpty()) {
                System.out.println("Dry-run: would delete " + metadataOrphans.size() + " orphan metadata files:");
                metadataOrphans.forEach(p -> System.out.println("  " + p));
            }
            printEmptyPartitionsDryRun(scan, dataOrphans);
            printEmptyTableDryRun(scan, dataOrphans, metadataOrphans);
            printReport(expiredIds, List.of(), List.of(), List.of());
            return;
        }

        CoolingPeriodFilter coolingFilter = new CoolingPeriodFilter(coolingDays);
        DirectoryGuard dirGuard = new DirectoryGuard();
        PhysicalDeletionService deletionService = new PhysicalDeletionService(s3Client, coolingFilter, dirGuard)
                .withLastModifiedCache(scan.l2Scanner().getLastModifiedCache());

        List<String> deletedData = List.of();
        if (!dataOrphans.isEmpty()) {
            deletedData = deletionService.deleteOrphans(dataOrphans);
            System.out.println("Deleted " + deletedData.size() + " orphan data files");
        } else {
            System.out.println("No orphan data files to delete.");
        }

        List<String> deletedMetadata = List.of();
        if (!metadataOrphans.isEmpty()) {
            deletedMetadata = deletionService.deleteOrphans(metadataOrphans);
            System.out.println("Deleted " + deletedMetadata.size() + " orphan metadata files");
        }

        Set<String> allDeletedFiles = new HashSet<>();
        allDeletedFiles.addAll(deletedData);
        allDeletedFiles.addAll(deletedMetadata);

        EmptyPartitionCleaner partitionCleaner = new EmptyPartitionCleaner(s3Client, dirGuard, deletionService);
        List<String> deletedPartitions = partitionCleaner.deleteEmptyPartitions(
                tableDataPrefix,
                scan.referencedFiles(),
                scan.prefixes(),
                allDeletedFiles);
        if (!deletedPartitions.isEmpty()) {
            System.out.println("Removed empty partition directories: " + deletedPartitions.size());
            deletedPartitions.forEach(p -> System.out.println("  " + p));
        } else {
            System.out.println("No empty partition directories to remove.");
        }

        handleEmptyTable(scan, deletionService, dirGuard);

        LOG.info("Cleanup complete: {} data orphans, {} metadata orphans, {} empty partitions",
                deletedData.size(), deletedMetadata.size(), deletedPartitions.size());
        printReport(expiredIds, deletedData, deletedMetadata, deletedPartitions);
    }

    private void handleEmptyTable(OrphanScanPipeline.Result scan,
                                  PhysicalDeletionService deletionService,
                                  DirectoryGuard dirGuard) {
        L2PhysicalScanner rescanner = new L2PhysicalScanner(s3Client);
        Set<String> physicalAfter = rescanner.listFiles(scan.prefixes());
        EmptyTableAnalyzer.Assessment assessment = EmptyTableAnalyzer.analyze(
                tableDataPrefix, scan.referencedFiles(), physicalAfter);

        if (!assessment.eligibleForTableCleanup()) {
            explainTableCleanupBlocked(assessment, scan.referencedFiles(), physicalAfter);
            return;
        }

        System.out.println("Table has no data files and no partitions on storage; eligible for table-level cleanup: "
                + assessment.tableRootPrefix());
        System.out.println("  metadata files still on storage: " + assessment.physicalMetadataFileCount());

        if (!purgeEmptyTables) {
            System.out.println("  Run list-empty-tables to review all empty tables, or re-run cleanup with --purge-empty-tables");
            return;
        }

        if (dropCatalog) {
            if (catalog == null || tableId == null) {
                System.err.println("  Cannot drop catalog entry: catalog not configured");
                return;
            }
            boolean dropped = catalog.dropTable(tableId);
            if (dropped) {
                System.out.println("Dropped table from catalog: " + tableId);
                LOG.info("Dropped empty table from catalog: {}", tableId);
            } else {
                System.err.println("Failed to drop table from catalog (may not exist): " + tableId);
                LOG.warn("dropTable returned false for table: {}", tableId);
                return;  // Don't purge storage if catalog drop failed
            }
        }

        EmptyTableCleaner tableCleaner = new EmptyTableCleaner(s3Client, dirGuard, deletionService);
        List<String> purged = tableCleaner.purgeEmptyTableStorage(assessment, physicalAfter);
        System.out.println("Purged " + purged.size() + " leftover storage object(s) for empty table");

        Set<String> remaining = tableCleaner.listRemainingObjects(tableDataPrefix);
        if (!remaining.isEmpty()) {
            System.out.println("  Warning: " + remaining.size() + " object(s) still remain under table root");
            return;
        }

        if (!dropCatalog) {
            System.out.println("  Storage purged. Add --drop-catalog to remove the catalog entry.");
        }
    }

    private void printEmptyTableDryRun(OrphanScanPipeline.Result scan,
                                       Set<String> dataOrphans,
                                       Set<String> metadataOrphans) {
        Set<String> physicalAfter = new HashSet<>(scan.physicalFiles());
        physicalAfter.removeAll(dataOrphans);
        physicalAfter.removeAll(metadataOrphans);

        Set<String> referenced = scan.referencedFiles();
        List<String> partitionDirsToRemove = EmptyPartitionCleaner.collectCandidates(
                        tableDataPrefix, scan.prefixes(), dataOrphans).stream()
                .filter(p -> EmptyPartitionCleaner.isDataPartitionPrefix(p, tableDataPrefix))
                .filter(p -> EmptyTableAnalyzer.isPartitionDeletable(p, referenced, physicalAfter))
                .sorted()
                .toList();
        physicalAfter.removeIf(path -> partitionDirsToRemove.stream()
                .anyMatch(prefix -> UriNormalizer.normalize(path).startsWith(
                        UriNormalizer.normalize(prefix))));

        EmptyTableAnalyzer.Assessment assessment = EmptyTableAnalyzer.analyze(
                tableDataPrefix, referenced, physicalAfter);
        if (!assessment.eligibleForTableCleanup()) {
            explainTableCleanupBlocked(assessment, referenced, physicalAfter);
            return;
        }
        System.out.println("Dry-run: table would be eligible for table-level cleanup after partition and orphan removal:");
        System.out.println("  " + assessment.tableRootPrefix());
        if (purgeEmptyTables) {
            if (dropCatalog) {
                System.out.println("  would drop catalog entry: " + table.name());
            }
            System.out.println("  would purge " + assessment.physicalMetadataFileCount()
                    + " metadata file(s) on storage");
        }
    }

    private void explainTableCleanupBlocked(EmptyTableAnalyzer.Assessment assessment,
                                            Set<String> referencedFiles,
                                            Set<String> physicalFiles) {
        if (assessment.hasReferencedData()) {
            System.out.println("Table-level cleanup blocked: metadata still references data files");
            return;
        }
        if (assessment.physicalDataFileCount() > 0) {
            System.out.println("Table-level cleanup blocked: "
                    + assessment.physicalDataFileCount() + " data file(s) remain on storage");
            return;
        }
        for (String partition : assessment.remainingPartitionPrefixes()) {
            if (!EmptyTableAnalyzer.isPartitionDeletable(partition, referencedFiles, physicalFiles)) {
                System.out.println("Table-level cleanup blocked: partition not yet deletable (stricter than table): "
                        + partition);
            } else {
                System.out.println("Table-level cleanup blocked: partition directory still on storage (remove partitions first): "
                        + partition);
            }
        }
    }

    private void printEmptyPartitionsDryRun(OrphanScanPipeline.Result scan, Set<String> dataOrphans) {
        Set<String> referenced = scan.referencedFiles();
        Set<String> candidates = EmptyPartitionCleaner.collectCandidates(
                tableDataPrefix, scan.prefixes(), dataOrphans);
        List<String> wouldClean = candidates.stream()
                .filter(p -> EmptyPartitionCleaner.isDataPartitionPrefix(p, tableDataPrefix))
                .filter(p -> !EmptyPartitionCleaner.hasReferencedDataFile(p, referenced))
                .sorted()
                .toList();
        if (!wouldClean.isEmpty()) {
            System.out.println("Dry-run: would remove empty partition directories: " + wouldClean.size());
            wouldClean.forEach(p -> System.out.println("  " + p));
        }
    }

    private void printReport(List<Long> expiredIds, List<String> deletedData,
                             List<String> deletedMetadata, List<String> deletedPartitions) {
        CleanupReport report = new CleanupReport(
                Instant.now(),
                table.name().toString(),
                expiredIds,
                deletedData,
                deletedMetadata,
                0L,
                List.of());
        System.out.println(report.summary());
        if (!deletedPartitions.isEmpty()) {
            System.out.println("  Empty partitions removed: " + deletedPartitions.size());
        }
    }
}
