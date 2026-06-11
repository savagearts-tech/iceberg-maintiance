package io.github.iceberg.cli;

import io.github.iceberg.cleanup.*;
import io.github.iceberg.common.CatalogLister;
import io.github.iceberg.common.JdbcCatalogConfig;
import io.github.iceberg.common.RetentionConfig;
import io.github.iceberg.common.TableFilter;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.LegacyMd5Plugin;

import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Main entry point for the Iceberg maintenance CLI.
 */
public class IcebergMaintenanceCli {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergMaintenanceCli.class);

    public static void main(String[] args) {
        HealthServer health = new HealthServer();
        health.start();

        if (args.length < 1) {
            printUsage();
            return;
        }

        String command = args[0];
        String tableName = resolveTableName(args);
        boolean allTables = containsFlag(args, "--all");
        int parallelism = parseParallelism(args);

        Properties config = loadConfig();
        int batchSize = parseBatchSize(args, config, parallelism);
        TableFilter tableFilter = resolveTableFilter(config, args);

        if (tableName == null && !allTables && !tableFilter.isRestrictive()) {
            tableName = config.getProperty("table.name", null);
        }

        try (JdbcCatalog catalog = new JdbcCatalogConfig(config).createCatalog()) {
            RetentionConfig retention = RetentionConfig.defaults();
            int coolingDays = Integer.parseInt(config.getProperty("coolingPeriodDays",
                    String.valueOf(RetentionConfig.DEFAULT_COOLING_PERIOD_DAYS)));

            if ("list-tables".equals(command)) {
                List<TableIdentifier> tables = CatalogLister.listTables(catalog, tableFilter);
                System.out.println("Tables in catalog (" + tables.size() + "):");
                for (TableIdentifier t : tables) {
                    System.out.println("  " + t);
                }
                return;
            }

            try (S3Client s3 = buildS3Client(config)) {

                if ("list-empty-tables".equals(command)) {
                    String warehouse = config.getProperty("warehouse");
                    new ListEmptyTablesCommand(catalog, tableFilter, warehouse, s3).execute();
                    return;
                }

                boolean batchMode = allTables || (tableName == null && tableFilter.isRestrictive());
                if (tableName == null && !batchMode) {
                    System.err.println("Error: specify a table name, use --all, or set catalog filters.");
                    System.err.println("  java -jar iceberg-cli.jar expire my_db.my_table");
                    System.err.println("  java -jar iceberg-cli.jar expire --all");
                    System.err.println("  java -jar iceberg-cli.jar expire --all --namespace alpha --table-prefix trace");
                    System.exit(1);
                }

                if (batchMode) {
                    Stream<TableIdentifier> tableStream = CatalogLister.streamTables(catalog, tableFilter);
                    System.out.println("Processing tables (parallelism=" + parallelism
                            + ", batchSize=" + batchSize + ")...");
                    ParallelMaintenanceExecutor executor = new ParallelMaintenanceExecutor(parallelism);
                    String warehouse = config.getProperty("warehouse");

                    ParallelMaintenanceExecutor.BatchResult result = executor.executeAll(
                            tableStream, command,
                            id -> {
                                try {
                                    executeCommand(command, catalog, id,
                                            deriveDataPrefix(warehouse, id), retention, coolingDays, s3, config, args);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            },
                            batchSize);

                    System.out.println(result.summary());
                    if (result.failed() > 0) {
                        System.err.println("Failed tables:");
                        for (var r : result.results()) {
                            if (!r.success()) {
                                System.err.println("  " + r.tableName() + ": " + r.errorMessage());
                            }
                        }
                    }
                    if ("cleanup".equals(command)) {
                        System.out.println();
                        new ListEmptyTablesCommand(catalog, tableFilter, warehouse, s3).execute();
                    }
                } else {
                    String warehouse = config.getProperty("warehouse");
                    String dataPrefix = config.getProperty("table.dataPrefix",
                            deriveDataPrefix(warehouse, TableIdentifier.parse(tableName)));
                    executeCommand(command, catalog, TableIdentifier.parse(tableName), dataPrefix,
                            retention, coolingDays, s3, config, args);
                }
            }

        } catch (Exception e) {
            LOG.error("Command failed: {}", command, e);
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }

    static TableFilter resolveTableFilter(Properties config, String[] args) {
        TableFilter fromConfig = TableFilter.fromProperties(config);
        TableFilter.Builder builder = TableFilter.builder();
        fromConfig.namespace().ifPresent(builder::namespace);
        fromConfig.tableNamePrefix().ifPresent(builder::tableNamePrefix);
        fromConfig.qualifiedNamePattern().ifPresent(p -> builder.qualifiedNamePattern(p.pattern()));

        String ns = getFlagValue(args, "--namespace");
        if (ns != null) {
            builder.namespace(ns);
        }
        String prefix = getFlagValue(args, "--table-prefix");
        if (prefix != null) {
            builder.tableNamePrefix(prefix);
        }
        String pattern = getFlagValue(args, "--table-pattern");
        if (pattern != null) {
            builder.qualifiedNamePattern(pattern);
        }
        return builder.build();
    }

    private static void executeCommand(String command, JdbcCatalog catalog, TableIdentifier tableId,
                                       String dataPrefix, RetentionConfig retention, int coolingDays,
                                       S3Client s3, Properties config, String[] args) throws Exception {
        Table table = catalog.loadTable(tableId);
        TableOperations tableOps = ((HasTableOperations) table).operations();

        boolean dryRun = Boolean.parseBoolean(config.getProperty("dryRun", "true"));
        boolean purgeEmptyTables = containsFlag(args, "--purge-empty-tables")
                || Boolean.parseBoolean(config.getProperty("purgeEmptyTables", "false"));
        boolean dropCatalog = containsFlag(args, "--drop-catalog")
                || Boolean.parseBoolean(config.getProperty("dropCatalog", "false"));
        boolean metadataOnly = containsFlag(args, "--metadata-only")
                || Boolean.parseBoolean(config.getProperty("metadataOnly", "false"));

        String scanWindowHoursStr = getFlagValue(args, "--time-scan-window-hours");
        List<String> explicitDataPrefixes = null;
        if (scanWindowHoursStr != null && !metadataOnly) {
            int scanWindowHours = Integer.parseInt(scanWindowHoursStr);
            io.github.iceberg.cleanup.scan.TimeWindowPrefixGenerator generator = 
                new io.github.iceberg.cleanup.scan.TimeWindowPrefixGenerator(table, dataPrefix);
            explicitDataPrefixes = generator.generate(coolingDays, scanWindowHours);
        }

        switch (command) {
            case "expire" -> new ExpireCommand(table, retention, dryRun).execute();
            case "scan-orphans" -> new ScanOrphansCommand(table, tableOps, dataPrefix, s3, null, explicitDataPrefixes, metadataOnly).execute();
            case "cleanup" -> new CleanupCommand(table, tableOps, dataPrefix, retention, s3, coolingDays, dryRun,
                    null, purgeEmptyTables, dropCatalog, catalog, tableId, explicitDataPrefixes, metadataOnly).execute();
            default -> printUsage();
        }
    }

    static String deriveDataPrefix(String warehouse, TableIdentifier tableId) {
        if (warehouse == null) return "";
        String base = warehouse.endsWith("/") ? warehouse : warehouse + "/";
        String ns = String.join("/", tableId.namespace().levels());
        String path = ns.isEmpty() ? tableId.name() : ns + "/" + tableId.name();
        return base + path + "/data";
    }

    private static S3Client buildS3Client(Properties config) {
        // LegacyMd5Plugin: DeleteObjects requires Content-MD5 on many S3-compatible stores (OSS, MinIO, COS).
        var builder = S3Client.builder()
                .region(Region.of(config.getProperty("s3.region", "us-east-1")))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .httpClient(ApacheHttpClient.builder().maxConnections(100).build())
                .addPlugin(LegacyMd5Plugin.create())
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED);

        String endpoint = config.getProperty("s3.endpoint");
        if (endpoint != null && !endpoint.isBlank()) {
            builder.endpointOverride(URI.create(endpoint));
        }

        boolean pathStyle = Boolean.parseBoolean(config.getProperty("s3.pathStyleAccess", "true"));
        builder.forcePathStyle(pathStyle);

        return builder.build();
    }

    private static boolean containsFlag(String[] args, String flag) {
        for (String a : args) {
            if (a.equals(flag)) return true;
        }
        return false;
    }

    private static int parseParallelism(String[] args) {
        String val = getFlagValue(args, "--parallelism");
        if (val != null) {
            int p = Integer.parseInt(val);
            if (p < 1) throw new IllegalArgumentException("parallelism must be >= 1, got: " + p);
            return p;
        }
        return Runtime.getRuntime().availableProcessors();
    }

    private static int parseBatchSize(String[] args, Properties config, int parallelism) {
        String val = getFlagValue(args, "--batch-size");
        if (val == null) {
            val = config.getProperty("catalog.batchSize");
        }
        if (val != null) {
            int bs = Integer.parseInt(val);
            if (bs < 1) throw new IllegalArgumentException("batch-size must be >= 1, got: " + bs);
            return bs;
        }
        return parallelism * 2;
    }

    private static String getFlagValue(String[] args, String flag) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(flag)) {
                return args[i + 1];
            }
        }
        return null;
    }

    /**
     * Resolves the table name from CLI arguments. The table name is the first
     * positional argument (non-flag, non-flag-value) after the command.
     */
    private static String resolveTableName(String[] args) {
        Set<String> flagKeys = Set.of("--namespace", "--table-prefix", "--table-pattern",
                "--parallelism", "--batch-size", "--time-scan-window-hours");
        for (int i = 1; i < args.length; i++) {
            if (args[i].startsWith("--")) {
                if (flagKeys.contains(args[i])) {
                    i++; // skip flag value
                }
                continue;
            }
            // This is a positional argument — treat as table name
            return args[i];
        }
        return null;
    }

    private static Properties loadConfig() {
        Properties props = new Properties();
        String[] keys = {
                "jdbc.url", "jdbc.driver", "jdbc.user", "jdbc.password",
                "warehouse", "table.name", "table.dataPrefix",
                "s3.region", "s3.endpoint", "s3.pathStyleAccess", "dryRun", "coolingPeriodDays",
                "catalog.namespace", "catalog.tablePrefix", "catalog.tablePattern", "catalog.batchSize",
                "purgeEmptyTables", "dropCatalog"
        };
        for (String key : keys) {
            String val = System.getProperty(key);
            if (val != null) {
                props.setProperty(key, val);
            }
            String envKey = "ICE_BERG_" + key.replace(".", "_").toUpperCase();
            String envVal = System.getenv(envKey);
            if (envVal != null) {
                props.setProperty(key, envVal);
            }
        }
        return props;
    }

    private static void printUsage() {
        System.out.println("""
                Iceberg Maintenance Tool

                Usage:
                  java -jar iceberg-cli.jar <command> [table-name|--all] [options]

                Commands:
                  expire              Expire old snapshots (dual retention)
                  scan-orphans        Scan for orphan (zombie) files
                  cleanup             Delete orphan files with safety checks
                  list-tables         List tables in the catalog
                  list-empty-tables   List tables with no data files (need table-level cleanup)

                Examples:
                  java -jar iceberg-cli.jar expire my_db.my_table
                  java -jar iceberg-cli.jar expire --all
                  java -jar iceberg-cli.jar list-empty-tables --all
                  java -jar iceberg-cli.jar cleanup --all --purge-empty-tables
                  java -jar iceberg-cli.jar cleanup my_db.my_table --metadata-only

                Options:
                  --all                    Process all tables (optionally narrowed by filters below)
                  --parallelism N          Max concurrent tables (default: CPU core count)
                  --batch-size N           Max in-flight tasks for backpressure (default: parallelism * 2)
                  --namespace NS           Limit to namespace, e.g. alpha or a.b
                  --table-prefix P         Table name prefix, e.g. trace_
                  --table-pattern R        Regex on fully-qualified table name
                  --time-scan-window-hours Auto-generate targeted S3 prefixes for L2 scan (e.g. 24)
                  --metadata-only          Surgical mode: skips data files, only scans and cleans metadata files
                  --purge-empty-tables     After cleanup, remove leftover storage for tables with no data
                  --drop-catalog           With --purge-empty-tables, also drop the table from JDBC catalog

                Configuration (system properties or env vars ICE_BERG_*):
                  jdbc.url              JDBC connection URL
                  jdbc.driver           JDBC driver class
                  jdbc.user             Database user
                  jdbc.password         Database password
                  warehouse             Iceberg warehouse path (e.g. s3a://bucket/warehouse)
                  table.name            Fully qualified table name (single-table mode)
                  table.dataPrefix      S3 prefix for table data (auto-derived when omitted)
                  catalog.namespace     Same as --namespace
                  catalog.tablePrefix   Same as --table-prefix
                  catalog.tablePattern  Same as --table-pattern
                  catalog.batchSize     Same as --batch-size
                  s3.region             AWS region (default: us-east-1)
                  s3.endpoint           S3-compatible endpoint URL (e.g. http://localhost:9000 for MinIO)
                  s3.pathStyleAccess    Use path-style addressing (default: true, required for MinIO/OSS/COS)
                  dryRun                true/false (default: true)
                  coolingPeriodDays     Cooling period in days (default: 3)
                  metadataOnly          true/false (default: false)
                  purgeEmptyTables      true/false (default: false)
                  dropCatalog           true/false (default: false)
                """);
    }
}
