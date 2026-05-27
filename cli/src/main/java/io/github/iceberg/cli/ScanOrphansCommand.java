package io.github.iceberg.cli;

import io.github.iceberg.cleanup.OrphanScanPipeline;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.expressions.Expression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Command to scan for orphan (zombie) files using the two-stage model.
 */
public class ScanOrphansCommand {

    private static final Logger LOG = LoggerFactory.getLogger(ScanOrphansCommand.class);

    private final Table table;
    private final TableOperations tableOperations;
    private final String tableDataPrefix;
    private final S3Client s3Client;
    private final Expression partitionFilter;

    private OrphanScanPipeline.Result lastResult;

    public ScanOrphansCommand(Table table, TableOperations tableOperations, String tableDataPrefix,
                              S3Client s3Client) {
        this(table, tableOperations, tableDataPrefix, s3Client, null);
    }

    public ScanOrphansCommand(Table table, TableOperations tableOperations, String tableDataPrefix,
                              S3Client s3Client, Expression partitionFilter) {
        this.table = table;
        this.tableOperations = tableOperations;
        this.tableDataPrefix = tableDataPrefix;
        this.s3Client = s3Client;
        this.partitionFilter = partitionFilter;
    }

    public OrphanScanPipeline.Result getLastResult() {
        return lastResult;
    }

    public void execute() {
        lastResult = new OrphanScanPipeline().execute(
                table, tableOperations, tableDataPrefix, s3Client, partitionFilter);

        System.out.println("L1 scan: " + lastResult.referencedFiles().size() + " files referenced in metadata");
        System.out.println("L2 scan: " + lastResult.physicalFiles().size() + " files on storage");
        System.out.println("Data orphans: " + lastResult.dataOrphans().size());
        System.out.println("Metadata orphans: " + lastResult.metadataOrphans().size());

        LOG.info("Scan-orphans complete: {} data orphans, {} metadata orphans",
                lastResult.dataOrphans().size(), lastResult.metadataOrphans().size());
    }
}
