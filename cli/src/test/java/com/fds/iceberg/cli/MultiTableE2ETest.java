package com.fds.iceberg.cli;

import com.fds.iceberg.common.CatalogLister;
import com.fds.iceberg.common.RetentionConfig;
import com.fds.iceberg.common.TableFilter;
import com.fds.iceberg.cleanup.SnapshotExpiryService;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.jdbc.JdbcClientPool;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.*;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;

import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.*;

/**
 * E2E test for multi-table catalog discovery + per-table maintenance.
 *
 * <p>Creates 3 tables, verifies all are discovered via {@link CatalogLister},
 * then runs snapshot expiry on each and validates results.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MultiTableE2ETest {

    private static final String MINIO_ENDPOINT = "http://localhost:9000";
    private static final String ACCESS_KEY = "minioadmin";
    private static final String SECRET_KEY = "minioadmin";
    private static final String BUCKET = "integration-test-bucket";
    private static final String WAREHOUSE = "s3a://" + BUCKET + "/multi-e2e";

    // Three tables in different namespaces/layouts
    private static final String TBL_ALPHA = "alpha.traces";      // ns=alpha, name=traces
    private static final String TBL_BETA = "beta.metrics";       // ns=beta,  name=metrics
    private static final String TBL_GAMMA = "standalone_events"; // no namespace

    private static JdbcCatalog catalog;
    private static S3Client s3;
    private static final List<String> createdTables = new ArrayList<>();

    @BeforeAll
    static void setup() {
        s3 = S3Client.builder()
                .endpointOverride(URI.create(MINIO_ENDPOINT))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .httpClient(UrlConnectionHttpClient.builder().build())
                .forcePathStyle(true)
                .build();
        ensureBucketExists(BUCKET);

        TestS3FileIO fileIO = new TestS3FileIO(s3);
        JdbcClientPool pool = new JdbcClientPool(
                "jdbc:h2:mem:iceberg_multi;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
                Map.of("user", "sa", "password", ""));

        catalog = new JdbcCatalog(
                props -> fileIO,
                props -> pool,
                true);
        catalog.initialize("iceberg_multi", Map.of(
                "warehouse", WAREHOUSE,
                "uri", "jdbc:h2:mem:iceberg_multi;MODE=PostgreSQL;DB_CLOSE_DELAY=-1"
        ));
    }

    @AfterAll
    static void teardown() {
        for (String name : createdTables) {
            try { catalog.dropTable(TableIdentifier.parse(name)); } catch (Exception ignored) {}
        }
        try { catalog.dropNamespace(org.apache.iceberg.catalog.Namespace.of("alpha")); } catch (Exception ignored) {}
        try { catalog.dropNamespace(org.apache.iceberg.catalog.Namespace.of("beta")); } catch (Exception ignored) {}
        s3.close();
    }

    // ═══════════════════════════════════════════
    //  Phase 1: Create 3 tables with data
    // ═══════════════════════════════════════════

    @Test
    @Order(1)
    @DisplayName("Create 3 tables with different namespaces and commit multiple snapshots")
    void createTables() {
        createTable(TBL_ALPHA, 3, "event_date", "day");   // 3 daily snapshots
        createTable(TBL_BETA,  4, "event_hour", "hour");   // 4 hourly snapshots
        createTable(TBL_GAMMA, 2, "ts", "day");            // 2 daily snapshots, root ns

        assertEquals(3, createdTables.size(), "All 3 tables should be created");
        System.out.println("=== Created " + createdTables.size() + " tables ===");
    }

    // ═══════════════════════════════════════════
    //  Phase 2: Catalog discovery
    // ═══════════════════════════════════════════

    @Test
    @Order(2)
    @DisplayName("CatalogLister discovers all 3 tables across namespaces")
    void discoverAllTables() {
        List<TableIdentifier> tables = CatalogLister.listAllTables(catalog);
        System.out.println("Discovered tables: " + tables);

        assertEquals(3, tables.size(), "Should discover 3 tables");
        assertTrue(tables.stream().anyMatch(t -> t.toString().equals(TBL_ALPHA)), "Should find " + TBL_ALPHA);
        assertTrue(tables.stream().anyMatch(t -> t.toString().equals(TBL_BETA)),  "Should find " + TBL_BETA);
        assertTrue(tables.stream().anyMatch(t -> t.toString().equals(TBL_GAMMA)), "Should find " + TBL_GAMMA);
    }

    @Test
    @Order(3)
    @DisplayName("CatalogLister filters tables by namespace and table prefix")
    void filterTables() {
        List<TableIdentifier> alphaTables = CatalogLister.listTables(catalog,
                TableFilter.builder().namespace("alpha").build());
        assertEquals(1, alphaTables.size());
        assertEquals(TBL_ALPHA, alphaTables.getFirst().toString());

        List<TableIdentifier> traceTables = CatalogLister.listTables(catalog,
                TableFilter.builder().tableNamePrefix("trace").build());
        assertEquals(1, traceTables.size());
        assertEquals(TBL_ALPHA, traceTables.getFirst().toString());

        List<TableIdentifier> betaMetrics = CatalogLister.listTables(catalog,
                TableFilter.builder().qualifiedNamePattern("beta\\.metrics").build());
        assertEquals(1, betaMetrics.size());
        assertEquals(TBL_BETA, betaMetrics.getFirst().toString());
    }

    // ═══════════════════════════════════════════
    //  Phase 3: Per-table snapshot expiry
    // ═══════════════════════════════════════════

    @Test
    @Order(4)
    @DisplayName("Expire snapshots on all discovered tables")
    void expireAllTables() {
        List<TableIdentifier> tables = CatalogLister.listAllTables(catalog);
        assertFalse(tables.isEmpty(), "Should have tables to process");

        for (TableIdentifier id : tables) {
            Table table = catalog.loadTable(id);
            String dataPrefix = IcebergMaintenanceCli.deriveDataPrefix(WAREHOUSE, id);
            System.out.println("Processing " + id + " (dataPrefix=" + dataPrefix + ")");

            // Count snapshots before
            long beforeCount = StreamSupport.stream(table.snapshots().spliterator(), false).count();
            System.out.println("  Snapshots before: " + beforeCount);

            // Expire: retain last 1 (coolingPeriodDays not used by SnapshotExpiryService)
            RetentionConfig config = new RetentionConfig(Duration.ZERO, 1, 1);
            SnapshotExpiryService svc = new SnapshotExpiryService(table, config);
            svc.expireSnapshots(false);

            // Verify: at most 1 snapshot remains
            long afterCount = StreamSupport.stream(table.snapshots().spliterator(), false).count();
            System.out.println("  Snapshots after:  " + afterCount);
            assertTrue(afterCount <= 1,
                    "Table " + id + " should have at most 1 snapshot after retainLast=1, got " + afterCount);

            // Verify data prefix matches expected Iceberg layout
            String expectedPrefix = WAREHOUSE + "/" + id.toString().replace('.', '/') + "/data";
            assertEquals(expectedPrefix, dataPrefix,
                    "Data prefix should follow Iceberg directory convention");
        }
    }

    // ═══════════════════════════════════════════
    //  helpers
    // ═══════════════════════════════════════════

    private static void ensureBucketExists(String bucket) {
        try {
            s3.headBucket(HeadBucketRequest.builder().bucket(bucket).build());
        } catch (Exception e) {
            s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        }
    }

    private static void createTable(String qualifiedName, int snapshotCount, String colName, String transform) {
        // Handle root-namespace tables (no dots)
        boolean hasNamespace = qualifiedName.contains(".");
        String nsName = hasNamespace ? qualifiedName.substring(0, qualifiedName.lastIndexOf('.')) : null;
        String tblName = hasNamespace ? qualifiedName.substring(qualifiedName.lastIndexOf('.') + 1) : qualifiedName;

        TableIdentifier id = hasNamespace
                ? TableIdentifier.of(org.apache.iceberg.catalog.Namespace.of(nsName.split("\\.")), tblName)
                : TableIdentifier.of(org.apache.iceberg.catalog.Namespace.empty(), tblName);

        // Create namespace if needed
        if (nsName != null) {
            try { catalog.createNamespace(org.apache.iceberg.catalog.Namespace.of(nsName)); } catch (Exception ignored) {}
        }

        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, colName, Types.StringType.get()),
                Types.NestedField.required(3, "value", Types.StringType.get())
        );
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity(colName)
                .build();

        Table table = catalog.createTable(id, schema, spec, Map.of("format-version", "2"));

        // Write snapshotCount data files
        String tableDir = hasNamespace ? (nsName + "/" + tblName) : tblName;
        for (int i = 0; i < snapshotCount; i++) {
            String partitionValue = "val-" + (i + 1);
            String partitionPath = colName + "=" + partitionValue;
            String objKey = "multi-e2e/" + tableDir + "/data/"
                    + partitionPath + "/part-" + String.format("%05d", i) + ".parquet";
            String s3Path = "s3a://" + BUCKET + "/" + objKey;

            s3.putObject(PutObjectRequest.builder().bucket(BUCKET).key(objKey).build(),
                    RequestBody.fromString("data-" + i));

            DataFile dataFile = DataFiles.builder(spec)
                    .withPath(s3Path)
                    .withFormat(FileFormat.PARQUET)
                    .withFileSizeInBytes(512)
                    .withRecordCount(1)
                    .withPartitionPath(partitionPath)
                    .build();

            table.newAppend().appendFile(dataFile).commit();
        }

        createdTables.add(qualifiedName);
        System.out.println("  Created " + qualifiedName + " with " + snapshotCount + " snapshots (" + id + ")");
    }
}
