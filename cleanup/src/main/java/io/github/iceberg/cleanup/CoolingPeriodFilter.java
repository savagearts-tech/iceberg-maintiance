package io.github.iceberg.cleanup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/**
 * Enforces a cooling period before physical file deletion.
 * Files modified within the configured cooling period are protected.
 *
 * <p>When a {@code lastModifiedCache} is provided (from {@link L2PhysicalScanner#getLastModified(String)}),
 * redundant S3 HEAD requests are avoided. Falls back to HEAD when cache has no entry.
 */
public class CoolingPeriodFilter {

    private static final Logger LOG = LoggerFactory.getLogger(CoolingPeriodFilter.class);

    private final Duration coolingPeriod;

    public CoolingPeriodFilter() {
        this(io.github.iceberg.common.RetentionConfig.DEFAULT_COOLING_PERIOD_DAYS);
    }

    public CoolingPeriodFilter(int coolingPeriodDays) {
        this.coolingPeriod = Duration.ofDays(coolingPeriodDays);
    }

    /**
     * Checks eligibility using the L2 scanner's lastModified cache when available.
     *
     * @param s3Path             the s3a:// path to check
     * @param s3Client           S3 client for HEAD fallback
     * @param lastModifiedCache  optional cache from L2 scan, may be null
     * @return {@code true} if the file is past the cooling period
     */
    public boolean isEligible(String s3Path, S3Client s3Client, Map<String, Instant> lastModifiedCache) {
        Instant lastModified = lastModifiedCache != null ? lastModifiedCache.get(s3Path) : null;

        if (lastModified == null) {
            // Fallback: HEAD request (slower �?happens for files not captured in L2 scan)
            try {
                String bucket = L2PhysicalScanner.extractBucket(s3Path);
                String key = L2PhysicalScanner.extractKey(s3Path);
                lastModified = s3Client.headObject(
                        HeadObjectRequest.builder().bucket(bucket).key(key).build()).lastModified();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("Interrupted while checking lastModified for {}: {}", s3Path, e.getMessage());
                return false;
            } catch (Exception e) {
                LOG.warn("Failed to check lastModified for {}: {}", s3Path, e.getMessage());
                return false;
            }
        }

        boolean eligible = lastModified.isBefore(Instant.now().minus(coolingPeriod));
        if (!eligible) {
            LOG.debug("File {} last modified {} is within cooling period ({})",
                    s3Path, lastModified, coolingPeriod);
        }
        return eligible;
    }

    /**
     * Legacy path �?performs a HEAD request for every file.
     */
    public boolean isEligible(String s3Path, S3Client s3Client) {
        return isEligible(s3Path, s3Client, null);
    }
}
