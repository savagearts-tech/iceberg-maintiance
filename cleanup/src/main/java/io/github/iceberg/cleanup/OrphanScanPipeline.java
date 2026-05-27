package io.github.iceberg.cleanup;

import io.github.iceberg.scan.PartitionedTableScanner;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.expressions.Expression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Single-pass orphan detection pipeline: one L1 metadata scan and one L2 physical scan.
 */
public class OrphanScanPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(OrphanScanPipeline.class);

    public record Result(
            Set<String> referencedFiles,
            Set<String> physicalFiles,
            List<String> prefixes,
            Set<String> dataOrphans,
            Set<String> metadataOrphans,
            L2PhysicalScanner l2Scanner
    ) {}

    public Result execute(Table table,
                          TableOperations tableOperations,
                          String tableDataPrefix,
                          S3Client s3Client,
                          Expression partitionFilter) {
        PartitionedTableScanner scanner = new PartitionedTableScanner(table, partitionFilter, tableDataPrefix);
        Set<String> referencedFiles = scanner.scanDataFiles();

        List<String> prefixes = new ArrayList<>(scanner.derivePrefixes());
        String metaPrefix = metadataPrefix(tableDataPrefix);
        prefixes.add(metaPrefix);

        L2PhysicalScanner l2Scanner = new L2PhysicalScanner(s3Client);
        Set<String> physicalFiles = l2Scanner.listFiles(prefixes);

        OrphanFileDetector detector = new OrphanFileDetector();
        Set<String> dataOrphans = detector.detectDataOrphans(physicalFiles, referencedFiles);

        MetadataReferenceChainWalker chainWalker =
                new MetadataReferenceChainWalker(table, tableOperations);
        Set<String> activeMetadata = chainWalker.buildActiveMetadataSet();
        Set<String> metadataOrphans = detector.detectMetadataOrphans(physicalFiles, activeMetadata);

        LOG.info("Orphan scan complete: {} data orphans, {} metadata orphans",
                dataOrphans.size(), metadataOrphans.size());

        return new Result(referencedFiles, physicalFiles, prefixes,
                dataOrphans, metadataOrphans, l2Scanner);
    }

    static String metadataPrefix(String tableDataPrefix) {
        String metaPrefix = tableDataPrefix.replace("/data", "/metadata");
        if (!metaPrefix.endsWith("/")) {
            metaPrefix += "/";
        }
        return metaPrefix;
    }
}
