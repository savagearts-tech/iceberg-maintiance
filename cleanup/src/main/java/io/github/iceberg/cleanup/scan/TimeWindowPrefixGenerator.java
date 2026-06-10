package io.github.iceberg.cleanup.scan;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class TimeWindowPrefixGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(TimeWindowPrefixGenerator.class);

    private final Table table;
    private final String tableDataPrefix;

    public TimeWindowPrefixGenerator(Table table, String tableDataPrefix) {
        this.table = table;
        this.tableDataPrefix = tableDataPrefix;
    }

    /**
     * Automatically generates S3 prefixes for the time window: [now - coolingDays - scanWindowHours, now - coolingDays]
     * Only works if the table has an Iceberg 'hour', 'day', or 'month' partition transform.
     */
    public List<String> generate(int coolingDays, int scanWindowHours) {
        PartitionSpec spec = table.spec();
        if (spec.isUnpartitioned()) {
            return List.of();
        }

        String timeField = null;
        String transformName = null;

        for (PartitionField field : spec.fields()) {
            String t = field.transform().toString();
            if (t.equals("hour") || t.equals("day") || t.equals("month")) {
                timeField = field.name();
                transformName = t;
                break; // Assume the first time-based field is the primary partition directory
            }
        }

        if (timeField == null) {
            LOG.warn("No 'hour', 'day' or 'month' transform found in partition spec for table {}. Auto prefix generation skipped.", table.name());
            return List.of();
        }

        LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));
        LocalDateTime end = now.minusDays(coolingDays);
        LocalDateTime start = end.minusHours(scanWindowHours);

        DateTimeFormatter formatter;
        if (transformName.equals("hour")) {
            formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH");
        } else if (transformName.equals("month")) {
            formatter = DateTimeFormatter.ofPattern("yyyy-MM");
        } else {
            formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        }

        int stepHours = transformName.equals("hour") ? 1 : 24;

        List<String> prefixes = new ArrayList<>();
        String base = tableDataPrefix.endsWith("/") ? tableDataPrefix : tableDataPrefix + "/";

        for (LocalDateTime dt = end; !dt.isBefore(start); dt = dt.minusHours(stepHours)) {
            String val = dt.format(formatter);
            String prefix = base + timeField + "=" + val + "/";
            if (!prefixes.contains(prefix)) {
                prefixes.add(prefix);
            }
        }

        LOG.info("Auto-generated {} time prefixes for L2 scan (transform={}, field={})", prefixes.size(), transformName, timeField);
        return prefixes;
    }
}
