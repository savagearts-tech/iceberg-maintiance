package com.fds.iceberg.common;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TableFilterTest {

    @Test
    void matchesAnyWhenEmpty() {
        TableFilter filter = TableFilter.any();
        assertTrue(filter.matches(TableIdentifier.of(Namespace.of("alpha"), "traces")));
        assertTrue(filter.matches(TableIdentifier.of(Namespace.empty(), "events")));
    }

    @Test
    void matchesNamespacePrefix() {
        TableFilter filter = TableFilter.builder().namespace("alpha").build();
        assertTrue(filter.matches(TableIdentifier.of(Namespace.of("alpha"), "traces")));
        assertFalse(filter.matches(TableIdentifier.of(Namespace.of("beta"), "metrics")));
    }

    @Test
    void matchesTableNamePrefix() {
        TableFilter filter = TableFilter.builder().tableNamePrefix("trace").build();
        assertTrue(filter.matches(TableIdentifier.of(Namespace.of("alpha"), "traces")));
        assertFalse(filter.matches(TableIdentifier.of(Namespace.of("beta"), "metrics")));
    }

    @Test
    void matchesQualifiedNamePattern() {
        TableFilter filter = TableFilter.builder().qualifiedNamePattern("alpha\\..*").build();
        assertTrue(filter.matches(TableIdentifier.of(Namespace.of("alpha"), "traces")));
        assertFalse(filter.matches(TableIdentifier.of(Namespace.of("beta"), "metrics")));
    }

    @Test
    void invalidPatternThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> TableFilter.builder().qualifiedNamePattern("[invalid").build());
    }
}
