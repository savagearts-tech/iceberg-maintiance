package io.github.iceberg.cleanup;

import io.github.iceberg.scan.FileSetBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * L1 logical expiry scanner â€?identifies data files not referenced by any
 * active snapshot via Iceberg metadata.
 *
 * <p>Uses {@link FileSetBuilder} to determine the "known good" set of files,
 * then compares against a provided set of candidate files (from L2 scan)
 * to identify orphans.
 */
public class L1LogicalExpiryScanner {

    private static final Logger LOG = LoggerFactory.getLogger(L1LogicalExpiryScanner.class);

    private final FileSetBuilder fileSetBuilder;

    public L1LogicalExpiryScanner(FileSetBuilder fileSetBuilder) {
        this.fileSetBuilder = fileSetBuilder;
    }

    /**
     * Returns the set of actively referenced file paths.
     */
    public Set<String> getReferencedFiles() {
        Set<String> referenced = fileSetBuilder.buildReferencedFileSet();
        LOG.info("L1 scan complete: {} actively referenced files", referenced.size());
        return referenced;
    }
}
