package com.fds.iceberg.common;

import java.time.Duration;

/**
 * Configuration POJO for Iceberg table retention policies.
 *
 * @param expireOlderThan snapshots older than this duration are eligible for expiry
 * @param retainLast      minimum number of recent snapshots to retain regardless of age
 * @param coolingPeriodDays  files modified within this many days are protected from physical deletion
 */
public record RetentionConfig(
        Duration expireOlderThan,
        int retainLast,
        int coolingPeriodDays
) {
    public static final Duration DEFAULT_EXPIRE_OLDER_THAN = Duration.ofDays(90);
    public static final int DEFAULT_RETAIN_LAST = 5;
    public static final int DEFAULT_COOLING_PERIOD_DAYS = 3;

    public static RetentionConfig defaults() {
        return new RetentionConfig(DEFAULT_EXPIRE_OLDER_THAN, DEFAULT_RETAIN_LAST, DEFAULT_COOLING_PERIOD_DAYS);
    }

    /**
     * Loads retention settings from CLI/system properties.
     */
    public static RetentionConfig fromProperties(java.util.Properties props) {
        long expireDays = Long.parseLong(props.getProperty("retention.expireDays",
                String.valueOf(DEFAULT_EXPIRE_OLDER_THAN.toDays())));
        int retainLast = Integer.parseInt(props.getProperty("retention.retainLast",
                String.valueOf(DEFAULT_RETAIN_LAST)));
        int coolingDays = Integer.parseInt(props.getProperty("coolingPeriodDays",
                String.valueOf(DEFAULT_COOLING_PERIOD_DAYS)));
        return new RetentionConfig(Duration.ofDays(expireDays), retainLast, coolingDays);
    }

    public RetentionConfig {
        if (retainLast < 1) {
            throw new IllegalArgumentException("retainLast must be >= 1, got: " + retainLast);
        }
        if (coolingPeriodDays < 1) {
            throw new IllegalArgumentException("coolingPeriodDays must be >= 1, got: " + coolingPeriodDays);
        }
    }
}
