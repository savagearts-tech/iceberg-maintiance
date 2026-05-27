package io.github.iceberg.cleanup;

import java.time.Instant;
import java.util.List;

/**
 * Immutable report of a cleanup operation.
 *
 * @param timestamp      when the cleanup was executed
 * @param tableName      the Iceberg table that was cleaned
 * @param expiredSnapshotIds snapshots that were expired
 * @param deletedDataFiles    orphan data files that were deleted
 * @param deletedMetadataFiles orphan metadata files that were deleted
 * @param totalBytesReclaimed estimated bytes reclaimed
 * @param errors              any errors encountered during the operation
 */
public record CleanupReport(
        Instant timestamp,
        String tableName,
        List<Long> expiredSnapshotIds,
        List<String> deletedDataFiles,
        List<String> deletedMetadataFiles,
        long totalBytesReclaimed,
        List<String> errors
) {

    /**
     * Returns a human-readable summary of this report.
     */
    public String summary() {
        return String.format(
                """
                Cleanup Report â€?%s
                  Table:          %s
                  Snapshots expired: %d
                  Data files deleted: %d
                  Metadata files deleted: %d
                  Bytes reclaimed: %d
                  Errors: %d
                """,
                timestamp, tableName,
                expiredSnapshotIds.size(),
                deletedDataFiles.size(),
                deletedMetadataFiles.size(),
                totalBytesReclaimed,
                errors.size()
        );
    }
}
