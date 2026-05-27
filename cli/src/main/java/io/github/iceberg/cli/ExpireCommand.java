package io.github.iceberg.cli;

import io.github.iceberg.cleanup.SnapshotExpiryService;
import io.github.iceberg.common.RetentionConfig;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Command to expire old snapshots based on dual retention policy.
 */
public class ExpireCommand {

    private static final Logger LOG = LoggerFactory.getLogger(ExpireCommand.class);

    private final Table table;
    private final RetentionConfig config;
    private final boolean dryRun;

    public ExpireCommand(Table table, RetentionConfig config, boolean dryRun) {
        this.table = table;
        this.config = config;
        this.dryRun = dryRun;
    }

    public void execute() {
        SnapshotExpiryService service = new SnapshotExpiryService(table, config);
        List<Long> expired = service.expireSnapshots(dryRun);
        if (dryRun) {
            LOG.info("Dry-run: {} snapshots eligible for expiry", expired.size());
            System.out.println("Dry-run: " + expired.size() + " snapshots would be expired.");
        } else {
            LOG.info("Expired {} snapshots", expired.size());
            System.out.println("Expired " + expired.size() + " snapshots.");
        }
    }
}
