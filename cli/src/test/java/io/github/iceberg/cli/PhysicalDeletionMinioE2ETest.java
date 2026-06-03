package io.github.iceberg.cli;

import io.github.iceberg.cleanup.CoolingPeriodFilter;
import io.github.iceberg.cleanup.DirectoryGuard;
import io.github.iceberg.cleanup.PhysicalDeletionService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * E2E: {@link PhysicalDeletionService} batch {@code DeleteObjects} against local MinIO.
 * Verifies {@code LegacyMd5Plugin} fixes "Missing required header: Content-Md5".
 */
class PhysicalDeletionMinioE2ETest {

    private static final String PREFIX = "warehouse/demo/data/e2e-batch-";
    private static final int KEY_COUNT = 5;

    private static S3Client s3;

    @BeforeAll
    static void setUp() {
        Assumptions.assumeTrue(MinioTestSupport.isReachable(),
                "MinIO not reachable at " + MinioTestSupport.ENDPOINT);
        s3 = MinioTestSupport.buildS3Client(true);
        MinioTestSupport.ensureBucket(s3);
    }

    @AfterAll
    static void tearDown() {
        if (s3 != null) {
            s3.close();
        }
    }

    @Test
    void batchDeleteOrphans_deletesMultipleKeysWithLegacyMd5() {
        Set<String> paths = seedOrphanFiles(KEY_COUNT);
        Map<String, Instant> lastModified = new LinkedHashMap<>();
        Instant past = Instant.now().minus(10, ChronoUnit.DAYS);
        paths.forEach(p -> lastModified.put(p, past));

        PhysicalDeletionService service = new PhysicalDeletionService(
                s3, new CoolingPeriodFilter(1), new DirectoryGuard())
                .withLastModifiedCache(lastModified);

        List<String> deleted = service.deleteOrphans(paths);

        assertEquals(KEY_COUNT, deleted.size(), "all keys should be deleted");
        paths.forEach(this::assertObjectAbsent);
    }

    private Set<String> seedOrphanFiles(int count) {
        Set<String> paths = new LinkedHashSet<>();
        for (int i = 0; i < count; i++) {
            String key = PREFIX + i + ".parquet";
            String path = "s3a://" + MinioTestSupport.BUCKET + "/" + key;
            s3.putObject(
                    PutObjectRequest.builder().bucket(MinioTestSupport.BUCKET).key(key).build(),
                    RequestBody.fromBytes(("orphan-" + i).getBytes()));
            paths.add(path);
            assertObjectPresent(path);
        }
        return paths;
    }

    private void assertObjectPresent(String s3aPath) {
        String bucket = MinioTestSupport.BUCKET;
        String key = s3aPath.substring(("s3a://" + bucket + "/").length());
        s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
    }

    private void assertObjectAbsent(String s3aPath) {
        String bucket = MinioTestSupport.BUCKET;
        String key = s3aPath.substring(("s3a://" + bucket + "/").length());
        assertThrows(NoSuchKeyException.class, () ->
                s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build()));
    }
}
