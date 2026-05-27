package com.fds.iceberg.common;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.List;

/**
 * Parses fully-qualified Iceberg table names into {@link TableIdentifier}.
 *
 * <p>Supports formats like {@code catalog.database.table} and {@code database.table}.
 */
public class TableIdentifierParser {

    private TableIdentifierParser() {}

    /**
     * Parses a dot-separated fully-qualified table name.
     *
     * @param fullyQualifiedName e.g. {@code iceberg_db.iceberg_tbl} or {@code my_catalog.iceberg_db.iceberg_tbl}
     * @return the parsed {@link TableIdentifier}
     * @throws IllegalArgumentException if the name has fewer than 2 parts
     */
    public static TableIdentifier parse(String fullyQualifiedName) {
        if (fullyQualifiedName == null || fullyQualifiedName.isBlank()) {
            throw new IllegalArgumentException("Table name must not be blank");
        }
        String[] parts = fullyQualifiedName.trim().split("\\.");
        if (parts.length < 2) {
            throw new IllegalArgumentException(
                    "Expected at least 'database.table', got: " + fullyQualifiedName);
        }
        String tableName = parts[parts.length - 1];
        Namespace namespace = Namespace.of(
                java.util.Arrays.copyOf(parts, parts.length - 1));
        return TableIdentifier.of(namespace, tableName);
    }

    /**
     * Extracts the table name from a fully-qualified name.
     */
    public static String tableName(String fullyQualifiedName) {
        return parse(fullyQualifiedName).name();
    }

    /**
     * Extracts the namespace parts from a fully-qualified name.
     */
    public static List<String> namespaceParts(String fullyQualifiedName) {
        return List.of(parse(fullyQualifiedName).namespace().levels());
    }
}
