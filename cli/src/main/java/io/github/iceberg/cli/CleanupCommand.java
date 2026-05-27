package io.github.iceberg.cli;

import io.github.iceberg.cleanup.*;
import io.github.iceberg.common.RetentionConfig;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.expressions.Expression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Instant;
import java.util.ArrayList;
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

    public CleanupCommand(Table table, TableOperations tableOperations, String tableDataPrefix,
                          RetentionConfig retentionConfig, S3Client s3Client, int coolingDays, boolean dryRun) {
        this(table, tableOperations, tableDataPrefix, retentionConfig, s3Client, coolingDays, dryRun, null);
    }

    public CleanupCommand(Table table, TableOperations tableOperations, String tableDataPrefix,
                          RetentionConfig retentionConfig, S3Client s3Client, int coolingDays,
                          boolean dryRun, Expression partitionFilter) {
        this.table = table;
        this.tableOperations = tableOperations;
        this.tableDataPrefix = tableDataPrefix;
        this.retentionConfig = retentionConfig;
        this.s3Client = s3Client;
        this.coolingDays = coolingDays;
        this.dryRun = dryRun;
        this.partitionFilter = partitionFilter;
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
                table, tableOperations, tableDataPrefix, s3Client, partitionFilter);

        System.out.println("L1 scan: " + scan.referencedFiles().size() + " files referenced in metadata");
        System.out.println("L2 scan: " + scan.physicalFiles().size() + " files on storage");
        System.out.println("Data orphans: " + scan.dataOrphans().size());
        System.out.println("Metadata orphans: " + scan.metadataOrphans().size());

        Set<String> orphans = scan.dataOrphans();
        if (orphans.isEmpty()) {
            System.out.println("No orphan data files to delete.");
            printReport(expiredIds, List.of(), List.of());
            return;
        }

        if (dryRun) {
            System.out.println("Dry-run: would delete " + orphans.size() + " orphan data files:");
            orphans.forEach(p -> System.out.println("  " + p));
            printReport(expiredIds, List.of(), new ArrayList<>(orphans));
            return;
        }

        CoolingPeriodFilter coolingFilter = new CoolingPeriodFilter(coolingDays);
        DirectoryGuard dirGuard = new DirectoryGuard();
        PhysicalDeletionService deletionService = new PhysicalDeletionService(s3Client, coolingFilter, dirGuard)
                .withLastModifiedCache(scan.l2Scanner().getLastModifiedCache());

        List<String> deleted = deletionService.deleteOrphans(orphans);
        System.out.println("Deleted " + deleted.size() + " orphan data files");
        LOG.info("Cleanup complete: {} orphans deleted", deleted.size());
        printReport(expiredIds, deleted, List.of());
    }

    private void printReport(List<Long> expiredIds, List<String> deleted, List<String> wouldDelete) {
        CleanupReport report = new CleanupReport(
                Instant.now(),
                table.name().toString(),
                expiredIds,
                deleted,
                List.of(),
                0L,
                List.of());
        System.out.println(report.summary());
        if (!wouldDelete.isEmpty()) {
            System.out.println("  Data files pending deletion (dry-run): " + wouldDelete.size());
        }
    }
}
