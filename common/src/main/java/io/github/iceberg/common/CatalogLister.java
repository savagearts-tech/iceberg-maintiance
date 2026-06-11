package io.github.iceberg.common;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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

    /**
     * Returns a lazy stream of tables matching the filter.
     *
     * <p>Unlike {@link #listTables}, this does not collect all tables into memory
     * before returning. Namespaces are traversed lazily using breadth-first search,
     * and tables are yielded one at a time. No sorting is applied.
     *
     * <p>Use this for batch processing large catalogs where holding all
     * {@link TableIdentifier} objects in memory is impractical.
     */
    public static Stream<TableIdentifier> streamTables(JdbcCatalog catalog, TableFilter filter) {
        TableFilter effective = filter != null ? filter : TableFilter.any();
        return StreamSupport.stream(
                new CatalogTableSpliterator(catalog, effective), false);
    }

    private static void collectTables(JdbcCatalog catalog, Namespace namespace, List<TableIdentifier> out) {
        out.addAll(catalog.listTables(namespace));
        for (Namespace child : catalog.listNamespaces(namespace)) {
            collectTables(catalog, child, out);
        }
    }

    /**
     * Lazy spliterator that traverses namespaces breadth-first and yields
     * tables matching the filter one at a time. Only one namespace's worth
     * of table identifiers is held in memory at any point.
     */
    private static class CatalogTableSpliterator extends Spliterators.AbstractSpliterator<TableIdentifier> {

        private final JdbcCatalog catalog;
        private final TableFilter filter;
        private final Deque<Namespace> namespaceQueue;
        private Iterator<TableIdentifier> currentBatch;

        CatalogTableSpliterator(JdbcCatalog catalog, TableFilter filter) {
            super(Long.MAX_VALUE, Spliterator.DISTINCT | Spliterator.NONNULL);
            this.catalog = catalog;
            this.filter = filter;
            this.namespaceQueue = new ArrayDeque<>();
            this.namespaceQueue.add(filter.scanRoot());
        }

        @Override
        public boolean tryAdvance(Consumer<? super TableIdentifier> action) {
            while (true) {
                if (currentBatch != null && currentBatch.hasNext()) {
                    TableIdentifier next = currentBatch.next();
                    if (filter.matches(next)) {
                        action.accept(next);
                        return true;
                    }
                    continue;
                }

                Namespace ns = namespaceQueue.poll();
                if (ns == null) {
                    return false;
                }

                try {
                    List<TableIdentifier> tables = catalog.listTables(ns);
                    currentBatch = tables.iterator();

                    List<Namespace> children = catalog.listNamespaces(ns);
                    for (Namespace child : children) {
                        namespaceQueue.add(child);
                    }
                } catch (Exception e) {
                    LOG.warn("Failed to list namespace {}: {}", ns, e.getMessage());
                    currentBatch = null;
                }
            }
        }
    }
}
