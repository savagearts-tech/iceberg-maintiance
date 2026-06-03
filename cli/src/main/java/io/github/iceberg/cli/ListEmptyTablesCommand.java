package io.github.iceberg.cli;

import io.github.iceberg.cleanup.EmptyTableAnalyzer;
import io.github.iceberg.cleanup.OrphanScanPipeline;
import io.github.iceberg.common.CatalogLister;
import io.github.iceberg.common.TableFilter;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Lists catalog tables that have no remaining data files/partitions and need table-level cleanup.
 */
public class ListEmptyTablesCommand {

    private static final Logger LOG = LoggerFactory.getLogger(ListEmptyTablesCommand.class);

    private final JdbcCatalog catalog;
    private final TableFilter tableFilter;
    private final String warehouse;
    private final S3Client s3Client;

    public ListEmptyTablesCommand(JdbcCatalog catalog, TableFilter tableFilter,
                                  String warehouse, S3Client s3Client) {
        this.catalog = catalog;
        this.tableFilter = tableFilter;
        this.warehouse = warehouse;
        this.s3Client = s3Client;
    }

    public List<EmptyTableAnalyzer.Assessment> execute() {
        List<TableIdentifier> tables = CatalogLister.listTables(catalog, tableFilter);
        List<EmptyTableAnalyzer.Assessment> emptyTables = new ArrayList<>();

        System.out.println("Scanning " + tables.size() + " tables for empty data...");
        for (TableIdentifier tableId : tables) {
            try {
                EmptyTableAnalyzer.Assessment assessment = assessTable(tableId);
                if (assessment.eligibleForTableCleanup()) {
                    emptyTables.add(assessment);
                }
            } catch (Exception e) {
                LOG.warn("Failed to assess table {}: {}", tableId, e.getMessage());
                System.err.println("  SKIP " + tableId + " (error: " + e.getMessage() + ")");
            }
        }

        emptyTables.sort(Comparator.comparing(EmptyTableAnalyzer.Assessment::tableDataPrefix));
        printReport(emptyTables, tables.size());
        return emptyTables;
    }

    EmptyTableAnalyzer.Assessment assessTable(TableIdentifier tableId) {
        Table table = catalog.loadTable(tableId);
        String dataPrefix = IcebergMaintenanceCli.deriveDataPrefix(warehouse, tableId);
        OrphanScanPipeline.Result scan = new OrphanScanPipeline().execute(
                table,
                ((HasTableOperations) table).operations(),
                dataPrefix,
                s3Client,
                null);
        return EmptyTableAnalyzer.analyze(dataPrefix, scan.referencedFiles(), scan.physicalFiles());
    }

    private void printReport(List<EmptyTableAnalyzer.Assessment> emptyTables, int totalScanned) {
        System.out.println();
        System.out.println("Empty tables needing table-level cleanup: " + emptyTables.size()
                + " / " + totalScanned + " scanned");
        if (emptyTables.isEmpty()) {
            System.out.println("  (none)");
            return;
        }
        for (EmptyTableAnalyzer.Assessment a : emptyTables) {
            System.out.printf("  %s  metadata_files_on_storage=%d%n",
                    a.tableRootPrefix(), a.physicalMetadataFileCount());
        }
        System.out.println();
        System.out.println("Run cleanup with --purge-empty-tables to remove leftover storage.");
        System.out.println("Add --drop-catalog to also remove the table from the JDBC catalog (destructive).");
    }
}
