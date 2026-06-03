package io.github.iceberg.cli;

import io.github.iceberg.cleanup.*;
import io.github.iceberg.common.RetentionConfig;
import io.github.iceberg.scan.FileSetBuilder;
import io.github.iceberg.scan.PartitionedTableScanner;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.jdbc.JdbcClientPool;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.*;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end integration test: local MinIO (S3) + H2 (JDBC catalog).
 *
 * <p>Covers the full pipeline:
 * table creation ???data commit ???snapshot expiry ???orphan detection ???physical deletion.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class IcebergMaintenanceE2ETest {

    private static final String MINIO_ENDPOINT = "http://localhost:9000";
    private static final String ACCESS_KEY = "minioadmin";
    private static final String SECRET_KEY = "minioadmin";
    private static final String BUCKET = "integration-test-bucket";
    private static final String WAREHOUSE = "s3a://" + BUCKET + "/e2e-test";
    private static final String TABLE_NAME = "e2e_db.e2e_tbl";
    private static final String DATA_PREFIX = WAREHOUSE + "/e2e_db/e2e_tbl/data";

    private static JdbcCatalog catalog;
    private static S3Client s3;
    private static Table table;

    @BeforeAll
    static void setup() {
        // ?????? S3/MinIO client ??????
        Assumptions.assumeTrue(MinioTestSupport.isReachable(),
                "MinIO not reachable at " + MINIO_ENDPOINT);
        s3 = MinioTestSupport.buildS3Client(true);
        ensureBucketExists(BUCKET);

        // ?????? H2-backed JdbcCatalog with custom S3 FileIO ??????
        TestS3FileIO fileIO = new TestS3FileIO(s3);

        JdbcClientPool pool = new JdbcClientPool(
                "jdbc:h2:mem:iceberg_e2e;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
                Map.of("user", "sa", "password", ""));

        catalog = new JdbcCatalog(
                props -> fileIO,
                props -> pool,
                true /* initialize catalog tables */);
        catalog.initialize("iceberg_e2e", Map.of(
                "warehouse", WAREHOUSE,
                "uri", "jdbc:h2:mem:iceberg_e2e;MODE=PostgreSQL;DB_CLOSE_DELAY=-1"
        ));
    }

    @AfterAll
    static void teardown() {
        try { catalog.dropTable(TableIdentifier.parse(TABLE_NAME)); } catch (Exception ignored) {}
        try { catalog.dropNamespace(org.apache.iceberg.catalog.Namespace.of("e2e_db")); } catch (Exception ignored) {}
        s3.close();
    }

    // ?????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????    //  Phase 1: Create table & commit snapshots
    // ?????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
    @Test
    @Order(1)
    @DisplayName("Create partitioned table via JDBC catalog + commit 5 daily snapshots")
    void createTableAndSnapshots() {
        catalog.createNamespace(org.apache.iceberg.catalog.Namespace.of("e2e_db"));

        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "event_date", Types.DateType.get()),
                Types.NestedField.required(3, "data", Types.StringType.get())
        );
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .day("event_date")
                .build();

        table = catalog.createTable(
                TableIdentifier.parse(TABLE_NAME), schema, spec,
                Map.of("format-version", "2"));
        assertNotNull(table);

        // Commit 5 daily snapshots with actual Iceberg DataFile entries
        String[] dates = {"2026-05-20", "2026-05-21", "2026-05-22", "2026-05-23", "2026-05-24"};
        for (int i = 0; i < dates.length; i++) {
            appendDataFile(dates[i], i + 1, spec);
        }

        long snapCount = StreamSupport.stream(table.snapshots().spliterator(), false).count();
        assertEquals(5, snapCount, "Should have 5 snapshots");
        System.out.println("=== Table created with " + snapCount + " snapshots");
    }

    // ?????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????    //  Phase 2: Snapshot expiry (dual retention)
    // ?????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
    @Test
    @Order(2)
    @DisplayName("Expire snapshots with retainLast=3 (keep 3 most recent)")
    void expireSnapshots() {
        RetentionConfig config = new RetentionConfig(Duration.ZERO, /*retainLast*/ 3, /*coolingDays*/ 1);
        SnapshotExpiryService svc = new SnapshotExpiryService(table, config);

        List<Long> dryRun = svc.dryRun();
        System.out.println("Dry-run eligible: " + dryRun.size());
        assertFalse(dryRun.isEmpty(), "Some snapshots should be eligible");

        List<Long> expired = svc.expireSnapshots(false);
        System.out.println("Expired: " + expired.size());

        long remaining = StreamSupport.stream(table.snapshots().spliterator(), false).count();
        assertTrue(remaining <= 3, "At most 3 snapshots should remain (retainLast=3)");
        System.out.println("Remaining snapshots: " + remaining);
    }

    // ?????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????    //  Phase 3: Scan + orphan detection
    // ?????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
    @Test
    @Order(3)
    @DisplayName("Scan table with partition filter and detect orphan files")
    void scanAndDetectOrphans() {
        // Reload table after expiry to ensure fresh metadata
        table = catalog.loadTable(TableIdentifier.parse(TABLE_NAME));
        long snapCount = StreamSupport.stream(table.snapshots().spliterator(), false).count();
        System.out.println("=== Snapshots before scan test: " + snapCount);
        // ?????? 3a. Partition pruning (full scan to detect all partitions) ??????
        PartitionedTableScanner scanner = new PartitionedTableScanner(
                table, null, DATA_PREFIX);

        assertTrue(scanner.isPartitioned());
        System.out.println("Partition columns: " + scanner.getPartitionColumns());
        assertEquals(List.of("event_date_day"), scanner.getPartitionColumns(),
                "Partition column from day(event_date) transform");

        List<String> prefixes = scanner.derivePrefixes();
        System.out.println("Derived L2 prefixes: " + prefixes);
        assertFalse(prefixes.isEmpty(), "Should derive at least one prefix from remaining snapshots");

        // ?????? 3b. Write orphan files to MinIO (not referenced by any snapshot) ??????
        // Use epoch-day paths to match Iceberg's partitionToPath format
        int orphanEpochDay = dateToEpochDay("2026-05-23");
        createOrphanFile("e2e-test/e2e_db/e2e_tbl/data/event_date_day=" + orphanEpochDay + "/orphan_d1.parquet");
        createOrphanFile("e2e-test/e2e_db/e2e_tbl/metadata/orphan_m0.metadata.json");

        // ?????? 3c. L1: logical expiry ??????
        L1LogicalExpiryScanner l1 = new L1LogicalExpiryScanner(new FileSetBuilder(table));
        Set<String> referenced = l1.getReferencedFiles();
        System.out.println("L1 referenced files: " + referenced.size());
        assertFalse(referenced.isEmpty(), "Should reference files from snapshots");

        // ?????? 3d. L2: physical scan (data/ + metadata/ prefixes) ??????
        L2PhysicalScanner l2 = new L2PhysicalScanner(s3);
        List<String> scanPrefixes = new ArrayList<>(scanner.derivePrefixes());
        // Also scan metadata directory for metadata orphan detection
        scanPrefixes.add(WAREHOUSE + "/e2e_db/e2e_tbl/metadata/");
        System.out.println("L2 scan prefixes: " + scanPrefixes);
        Set<String> physical = l2.listFiles(scanPrefixes);
        System.out.println("L2 physical files: " + physical.size());
        // Note: ExpireSnapshots.cleanExpiredFiles(true) in test 2 may have deleted
        // data files from S3, so physical may be empty or smaller than expected.

        // ?????? 3e. Detect data orphans ??????
        OrphanFileDetector detector = new OrphanFileDetector();
        Set<String> dataOrphans = detector.detectDataOrphans(physical, referenced);
        System.out.println("Data orphans: " + dataOrphans.size());
        assertTrue(dataOrphans.stream().anyMatch(p -> p.contains("orphan_d")),
                "Should detect orphan data files");

        // ?????? 3f. Metadata reference chain ??????
        MetadataReferenceChainWalker walker = new MetadataReferenceChainWalker(
                table, ((org.apache.iceberg.BaseTable) table).operations());
        Set<String> activeMeta = walker.buildActiveMetadataSet();
        System.out.println("Active metadata files: " + activeMeta.size());

        Set<String> metaOrphans = detector.detectMetadataOrphans(physical, activeMeta);
        System.out.println("Metadata orphans: " + metaOrphans.size());
        for (String active : activeMeta) {
            assertFalse(metaOrphans.contains(active),
                    "Reachable metadata file must not be flagged as orphan: " + active);
        }
        assertTrue(metaOrphans.stream().anyMatch(p -> p.contains("orphan_m0")),
                "Should detect orphan metadata file");
    }

    // ?????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????    //  Phase 4: Physical deletion with safety
    // ?????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
    @Test
    @Order(4)
    @DisplayName("Delete orphans through PhysicalDeletionService with safety checks")
    void deleteOrphans() {
        // Re-detect orphans (clean state)
        L1LogicalExpiryScanner l1 = new L1LogicalExpiryScanner(new FileSetBuilder(table));
        Set<String> referenced = l1.getReferencedFiles();

        L2PhysicalScanner l2 = new L2PhysicalScanner(s3);
        Set<String> physical = l2.listFiles(
                new PartitionedTableScanner(table, null, DATA_PREFIX).derivePrefixes());

        OrphanFileDetector detector = new OrphanFileDetector();
        Set<String> orphans = detector.detectDataOrphans(physical, referenced);
        System.out.println("Data orphans to delete: " + orphans);

        if (orphans.isEmpty()) {
            System.out.println("No orphans to delete ???skipping deletion test");
            return;
        }

        // Deletion with safety filters (0-day cooling = no cooling period)
        CoolingPeriodFilter noCooling = new CoolingPeriodFilter(0);
        DirectoryGuard dirGuard = new DirectoryGuard();
        PhysicalDeletionService delSvc = new PhysicalDeletionService(s3, noCooling, dirGuard);
        List<String> deleted = delSvc.deleteOrphans(orphans);

        System.out.println("Deleted " + deleted.size() + " files");
        assertFalse(deleted.isEmpty(), "Should delete files");

        // Verify: re-scan should show fewer files
        Set<String> afterPhysical = l2.listFiles(
                new PartitionedTableScanner(table, null, DATA_PREFIX).derivePrefixes());
        Set<String> remainingOrphans = detector.detectDataOrphans(afterPhysical, referenced);
        assertTrue(remainingOrphans.isEmpty(), "All data orphans should be gone after deletion");
    }

    @Test
    @Order(5)
    @DisplayName("Remove empty partition directory after orphan data file deletion")
    void deleteEmptyPartitionDirectory() {
        String partitionKey = "e2e-test/e2e_db/e2e_tbl/data/event_date_day=99999/";
        String orphanKey = partitionKey + "orphan-only.parquet";
        createOrphanFile(orphanKey);
        s3.putObject(PutObjectRequest.builder().bucket(BUCKET).key(partitionKey).build(),
                RequestBody.empty());

        L1LogicalExpiryScanner l1 = new L1LogicalExpiryScanner(new FileSetBuilder(table));
        Set<String> referenced = l1.getReferencedFiles();
        String orphanPath = "s3a://" + BUCKET + "/" + orphanKey;
        String partitionPrefix = "s3a://" + BUCKET + "/" + partitionKey;

        CoolingPeriodFilter noCooling = new CoolingPeriodFilter(0);
        DirectoryGuard dirGuard = new DirectoryGuard();
        PhysicalDeletionService delSvc = new PhysicalDeletionService(s3, noCooling, dirGuard);
        List<String> deleted = delSvc.deleteOrphans(Set.of(orphanPath));
        assertEquals(1, deleted.size());

        EmptyPartitionCleaner partitionCleaner = new EmptyPartitionCleaner(s3, dirGuard, delSvc);
        List<String> cleaned = partitionCleaner.deleteEmptyPartitions(
                DATA_PREFIX, referenced, List.of(partitionPrefix), deleted);
        assertTrue(cleaned.contains(partitionPrefix), "Should remove empty partition prefix");

        assertThrows(NoSuchKeyException.class, () ->
                s3.headObject(HeadObjectRequest.builder().bucket(BUCKET).key(partitionKey).build()));
    }

    // ?????? helpers ??????

    private static void ensureBucketExists(String bucket) {
        try {
            s3.headBucket(HeadBucketRequest.builder().bucket(bucket).build());
        } catch (Exception e) {
            s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        }
    }

    /** Convert a date string (yyyy-MM-dd) to Iceberg's epoch-day partition value. */
    private static int dateToEpochDay(String date) {
        return (int) java.time.LocalDate.parse(date).toEpochDay();
    }

    /** Append a valid Iceberg DataFile entry + matching S3 object using epoch-day paths. */
    private void appendDataFile(String date, long id, PartitionSpec spec) {
        int epochDay = dateToEpochDay(date);
        String partitionPath = "event_date_day=" + epochDay;
        String objKey = "e2e-test/e2e_db/e2e_tbl/data/" + partitionPath + "/part-" + String.format("%05d", id) + ".parquet";
        String s3Path = "s3a://" + BUCKET + "/" + objKey;

        // Write a dummy file to MinIO so L2 scanner can list it
        s3.putObject(PutObjectRequest.builder().bucket(BUCKET).key(objKey).build(),
                RequestBody.fromString("dummy-" + id));

        // Build a valid Iceberg DataFile descriptor and commit it.
        // withPartitionPath accepts date strings for day(ts) transforms and
        // converts them to epoch days internally. The S3 path must use epoch
        // days to match Iceberg's partitionToPath output.
        DataFile dataFile = DataFiles.builder(spec)
                .withPath(s3Path)
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(1024)
                .withRecordCount(1)
                .withPartitionPath("event_date_day=" + date)  // Iceberg converts this
                .build();

        table.newAppend().appendFile(dataFile).commit();
        System.out.println("  Appended: " + s3Path);
    }

    private void createOrphanFile(String key) {
        s3.putObject(PutObjectRequest.builder().bucket(BUCKET).key(key).build(),
                RequestBody.fromString("orphan"));
        System.out.println("  Created orphan: " + key);
    }
}
