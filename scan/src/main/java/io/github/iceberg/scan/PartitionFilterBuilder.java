package io.github.iceberg.scan;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;

/**
 * Builds partition filter expressions for maintenance scans from a look-back window.
 *
 * <p>The filter selects partitions <em>older than</em> the look-back window,
 * which is the typical scope for cleanup operations (expired snapshots, orphan files).
 */
public final class PartitionFilterBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionFilterBuilder.class);

    private PartitionFilterBuilder() {}

    /**
     * Returns a filter selecting partitions older than {@code partitionDays},
     * or {@code null} when no filter should be applied.
     * <p>
     * Example: {@code partitionDays=30} on May 26 produces
     * {@code event_date &lt; 2026-04-26}, returning only partitions before April 26.
     *
     * @param partitionDays  number of days to look back; �?0 means no filter
     */
    public static Expression build(Table table, int partitionDays) {
        if (partitionDays <= 0) {
            return null;
        }
        PartitionSpec spec = table.spec();
        if (spec.isUnpartitioned()) {
            LOG.warn("maintenance.partitionDays set but table {} is unpartitioned; ignoring filter", table.name());
            return null;
        }

        PartitionField field = spec.fields().getFirst();
        String sourceColumn = table.schema().findColumnName(field.sourceId());
        if (sourceColumn == null) {
            LOG.warn("Could not resolve source column for partition field {}; skipping filter", field.name());
            return null;
        }

        Type sourceType = table.schema().findType(field.sourceId());
        LocalDate cutoff = LocalDate.now().minusDays(partitionDays);
        Expression filter = filterForType(sourceColumn, sourceType, cutoff);
        LOG.info("Partition filter: {} < {} (older than {} days)", sourceColumn, cutoff, partitionDays);
        return filter;
    }

    private static Expression filterForType(String column, Type type, LocalDate cutoff) {
        if (type instanceof Types.DateType) {
            return Expressions.lessThan(column, cutoff.toEpochDay());
        }
        if (type instanceof Types.TimestampType) {
            return Expressions.lessThan(column, cutoff.atStartOfDay()
                    .toInstant(java.time.ZoneOffset.UTC).toEpochMilli() * 1000L);
        }
        if (type instanceof Types.TimestampNanoType) {
            return Expressions.lessThan(column, cutoff.atStartOfDay()
                    .toInstant(java.time.ZoneOffset.UTC).toEpochMilli() * 1_000_000L);
        }
        if (type instanceof Types.StringType) {
            return Expressions.lessThan(column, cutoff.toString());
        }
        LOG.warn("Unsupported partition source type {} for column {}; skipping filter", type, column);
        return null;
    }
}
