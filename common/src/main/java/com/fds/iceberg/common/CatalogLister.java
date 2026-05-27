package com.fds.iceberg.common;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Discovers Iceberg tables from a JDBC catalog with optional naming filters.
 */
public class CatalogLister {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogLister.class);

    private CatalogLister() {}

    /**
     * Lists all tables across all namespaces (recursive).
     */
    public static List<TableIdentifier> listAllTables(JdbcCatalog catalog) {
        return listTables(catalog, TableFilter.any());
    }

    /**
     * Lists tables under the scan root, then applies {@link TableFilter} naming rules.
     */
    public static List<TableIdentifier> listTables(JdbcCatalog catalog, TableFilter filter) {
        TableFilter effective = filter != null ? filter : TableFilter.any();
        List<TableIdentifier> discovered = new ArrayList<>();
        collectTables(catalog, effective.scanRoot(), discovered);

        List<TableIdentifier> matched = discovered.stream()
                .filter(effective::matches)
                .sorted(Comparator.comparing(TableIdentifier::toString))
                .toList();

        LOG.info("Discovered {} tables in catalog ({} matched filter)",
                discovered.size(), matched.size());
        return matched;
    }

    private static void collectTables(JdbcCatalog catalog, Namespace namespace, List<TableIdentifier> out) {
        out.addAll(catalog.listTables(namespace));
        for (Namespace child : catalog.listNamespaces(namespace)) {
            collectTables(catalog, child, out);
        }
    }
}
