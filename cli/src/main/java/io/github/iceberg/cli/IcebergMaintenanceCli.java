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
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;

import java.util.List;
import java.util.Properties;

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
        String tableName = args.length > 1 && !args[1].startsWith("--") ? args[1] : null;
        boolean allTables = containsFlag(args, "--all");
        int parallelism = parseParallelism(args);

        Properties config = loadConfig();
        TableFilter tableFilter = resolveTableFilter(config, args);

        if (tableName == null && !allTables && !tableFilter.isRestrictive()) {
            tableName = config.getProperty("table.name", null);
        }

        try {
            JdbcCatalog catalog = new JdbcCatalogConfig(config).createCatalog();
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

            boolean batchMode = allTables || (tableName == null && tableFilter.isRestrictive());
            if (tableName == null && !batchMode) {
                System.err.println("Error: specify a table name, use --all, or set catalog filters.");
                System.err.println("  java -jar iceberg-cli.jar expire my_db.my_table");
                System.err.println("  java -jar iceberg-cli.jar expire --all");
                System.err.println("  java -jar iceberg-cli.jar expire --all --namespace alpha --table-prefix trace");
                System.exit(1);
            }

            S3Client s3 = buildS3Client(config);

            if (batchMode) {
                List<TableIdentifier> tables = CatalogLister.listTables(catalog, tableFilter);
                if (tables.isEmpty()) {
                    System.out.println("No tables matched the catalog filter.");
                    s3.close();
                    return;
                }
                System.out.println("Processing " + tables.size() + " tables (parallelism=" + parallelism + ")...");
                ParallelMaintenanceExecutor executor = new ParallelMaintenanceExecutor(parallelism);
                org.apache.iceberg.catalog.TableIdentifier firstId = tables.getFirst();
                String warehouse = config.getProperty("warehouse");
                String dataPrefix = deriveDataPrefix(warehouse, firstId);

                ParallelMaintenanceExecutor.BatchResult result = executor.executeAll(
                        tables, command,
                        id -> {
                            try {
                                executeCommand(command, catalog, id,
                                        deriveDataPrefix(warehouse, id), retention, coolingDays, s3, config);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

                System.out.println(result.summary());
                if (result.failed() > 0) {
                    System.err.println("Failed tables:");
                    for (var r : result.results()) {
                        if (!r.success()) {
                            System.err.println("  " + r.tableName() + ": " + r.errorMessage());
                        }
                    }
                }
            } else {
                String warehouse = config.getProperty("warehouse");
                String dataPrefix = config.getProperty("table.dataPrefix",
                        deriveDataPrefix(warehouse, TableIdentifier.parse(tableName)));
                executeCommand(command, catalog, TableIdentifier.parse(tableName), dataPrefix,
                        retention, coolingDays, s3, config);
            }

            s3.close();

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
                                       S3Client s3, Properties config) throws Exception {
        Table table = catalog.loadTable(tableId);
        TableOperations tableOps = ((HasTableOperations) table).operations();

        boolean dryRun = Boolean.parseBoolean(config.getProperty("dryRun", "true"));

        switch (command) {
            case "expire" -> new ExpireCommand(table, retention, dryRun).execute();
            case "scan-orphans" -> new ScanOrphansCommand(table, tableOps, dataPrefix, s3).execute();
            case "cleanup" -> new CleanupCommand(table, tableOps, dataPrefix, retention, s3, coolingDays, dryRun).execute();
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
        return S3Client.builder()
                .region(Region.of(config.getProperty("s3.region", "us-east-1")))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .httpClient(UrlConnectionHttpClient.builder().build())
                .build();
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

    private static String getFlagValue(String[] args, String flag) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(flag)) {
                return args[i + 1];
            }
        }
        return null;
    }

    private static Properties loadConfig() {
        Properties props = new Properties();
        String[] keys = {
                "jdbc.url", "jdbc.driver", "jdbc.user", "jdbc.password",
                "warehouse", "table.name", "table.dataPrefix",
                "s3.region", "dryRun", "coolingPeriodDays",
                "catalog.namespace", "catalog.tablePrefix", "catalog.tablePattern"
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
                  expire        Expire old snapshots (dual retention)
                  scan-orphans  Scan for orphan (zombie) files
                  cleanup       Delete orphan files with safety checks
                  list-tables   List tables in the catalog

                Examples:
                  java -jar iceberg-cli.jar expire my_db.my_table
                  java -jar iceberg-cli.jar expire --all
                  java -jar iceberg-cli.jar expire --all --namespace alpha --table-prefix trace
                  java -jar iceberg-cli.jar list-tables --namespace fds_db --table-pattern "fds_db\\.trace_.*"

                Options:
                  --all              Process all tables (optionally narrowed by filters below)
                  --parallelism N    Max concurrent tables (default: CPU core count)
                  --namespace NS     Limit to namespace, e.g. alpha or a.b
                  --table-prefix P   Table name prefix, e.g. trace_
                  --table-pattern R  Regex on fully-qualified table name

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
                  s3.region             AWS region (default: us-east-1)
                  dryRun                true/false (default: true)
                  coolingPeriodDays     Cooling period in days (default: 3)
                """);
    }
}
