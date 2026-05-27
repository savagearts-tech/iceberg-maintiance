package io.github.iceberg.common;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Client-side filter for catalog table discovery.
 *
 * <p>Iceberg {@code Catalog} APIs list tables by namespace only; naming rules
 * are applied after listing via this filter.
 */
public record TableFilter(
        Optional<String> namespace,
        Optional<String> tableNamePrefix,
        Optional<Pattern> qualifiedNamePattern
) {

    public static TableFilter any() {
        return new TableFilter(Optional.empty(), Optional.empty(), Optional.empty());
    }

    public static TableFilter fromProperties(Properties props) {
        if (props == null) {
            return any();
        }
        return builder()
                .namespace(blankToNull(props.getProperty("catalog.namespace")))
                .tableNamePrefix(blankToNull(props.getProperty("catalog.tablePrefix")))
                .qualifiedNamePattern(blankToNull(props.getProperty("catalog.tablePattern")))
                .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public boolean matches(TableIdentifier id) {
        if (namespace.isPresent()) {
            String required = namespace.get();
            String actual = namespaceString(id.namespace());
            if (!actual.equals(required) && !actual.startsWith(required + ".")) {
                return false;
            }
        }
        if (tableNamePrefix.isPresent() && !id.name().startsWith(tableNamePrefix.get())) {
            return false;
        }
        if (qualifiedNamePattern.isPresent()
                && !qualifiedNamePattern.get().matcher(id.toString()).matches()) {
            return false;
        }
        return true;
    }

    public Namespace scanRoot() {
        return namespace.map(ns -> Namespace.of(ns.split("\\."))).orElse(Namespace.empty());
    }

    /** Returns true when any naming constraint is configured. */
    public boolean isRestrictive() {
        return namespace.isPresent() || tableNamePrefix.isPresent() || qualifiedNamePattern.isPresent();
    }

    private static String namespaceString(Namespace ns) {
        return ns.isEmpty() ? "" : String.join(".", ns.levels());
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    public static final class Builder {
        private String namespace;
        private String tableNamePrefix;
        private Pattern qualifiedNamePattern;

        public Builder namespace(String namespace) {
            this.namespace = blankToNull(namespace);
            return this;
        }

        public Builder tableNamePrefix(String tableNamePrefix) {
            this.tableNamePrefix = blankToNull(tableNamePrefix);
            return this;
        }

        public Builder qualifiedNamePattern(String pattern) {
            String trimmed = blankToNull(pattern);
            if (trimmed == null) {
                this.qualifiedNamePattern = null;
            } else {
                try {
                    this.qualifiedNamePattern = Pattern.compile(trimmed);
                } catch (PatternSyntaxException e) {
                    throw new IllegalArgumentException("Invalid catalog.tablePattern: " + trimmed, e);
                }
            }
            return this;
        }

        public TableFilter build() {
            return new TableFilter(
                    Optional.ofNullable(namespace),
                    Optional.ofNullable(tableNamePrefix),
                    Optional.ofNullable(qualifiedNamePattern));
        }
    }
}
