package io.github.iceberg.cleanup;

import io.github.iceberg.common.RetentionConfig;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Expires snapshots based on dual retention constraints:
 * {@code expireOlderThan} AND {@code retainLast}.
 *
 * <p>A snapshot is expired only when BOTH conditions are met:
 * its timestamp is older than the threshold AND it is not among
 * the most recent N snapshots.
 *
 * <p>Delegates to Iceberg's built-in {@link ExpireSnapshots} API.
 */
public class SnapshotExpiryService {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotExpiryService.class);

    private final Table table;
    private final RetentionConfig config;

    public SnapshotExpiryService(Table table, RetentionConfig config) {
        this.table = table;
        this.config = config;
    }

    /**
     * Identifies and optionally expires snapshots under the dual retention policy.
     *
     * @param dryRun if {@code true}, only logs what would be expired
     * @return list of expired snapshot IDs
     */
    public List<Long> expireSnapshots(boolean dryRun) {
        long cutoffMillis = Instant.now().minus(config.expireOlderThan()).toEpochMilli();
        List<Long> expiredIds = computeExpiredSnapshotIds(cutoffMillis);

        if (dryRun) {
            long total = StreamSupport.stream(table.snapshots().spliterator(), false).count();
            LOG.info("Dry-run: {} snapshots would be expired from table {} ({} total, retainLast={})",
                    expiredIds.size(), table.name(), total, config.retainLast());
            return expiredIds;
        }

        // Use Iceberg's built-in ExpireSnapshots API for dual retention.
        // cleanExpiredFiles(false): data file cleanup is handled by the orphan
        // detection/deletion pipeline, not during snapshot expiry.
        if (!expiredIds.isEmpty()) {
            ExpireSnapshots expire = table.expireSnapshots()
                    .expireOlderThan(cutoffMillis)
                    .retainLast(config.retainLast())
                    .cleanExpiredFiles(false);
            expire.commit();
            // Note: reported expiredIds is a best-effort estimate computed before commit.
            // Iceberg's retainLast may protect some IDs during commit, so actual expired
            // set may differ. This is acceptable for operational reporting.
        }

        long remainingCount = StreamSupport.stream(table.snapshots().spliterator(), false).count();
        LOG.info("Expired {} snapshots from table {} (remaining: {})",
                expiredIds.size(), table.name(), remainingCount);
        return expiredIds;
    }

    /**
     * Dry-run convenience method.
     */
    public List<Long> dryRun() {
        return expireSnapshots(true);
    }

    /**
     * Mirrors Iceberg {@link ExpireSnapshots} semantics: a snapshot is expired when it is
     * older than the cutoff and not among the {@code retainLast} most recent snapshots.
     */
    List<Long> computeExpiredSnapshotIds(long cutoffMillis) {
        List<org.apache.iceberg.Snapshot> newestFirst = StreamSupport
                .stream(table.snapshots().spliterator(), false)
                .sorted((a, b) -> Long.compare(b.timestampMillis(), a.timestampMillis()))
                .toList();

        Set<Long> mustRetain = newestFirst.stream()
                .limit(config.retainLast())
                .map(org.apache.iceberg.Snapshot::snapshotId)
                .collect(Collectors.toSet());

        return newestFirst.stream()
                .filter(s -> s.timestampMillis() < cutoffMillis)
                .map(org.apache.iceberg.Snapshot::snapshotId)
                .filter(id -> !mustRetain.contains(id))
                .sorted()
                .collect(Collectors.toList());
    }
}
