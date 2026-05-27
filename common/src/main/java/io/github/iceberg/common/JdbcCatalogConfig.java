package io.github.iceberg.common;

import org.apache.iceberg.jdbc.JdbcCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Loads JDBC connection properties and initializes Iceberg's built-in {@link JdbcCatalog}.
 *
 * <p>Expected configuration keys:
 * <ul>
 *   <li>{@code uri} â€?JDBC connection URL (or {@code jdbc.url})</li>
 *   <li>{@code warehouse} â€?Iceberg warehouse location (required)</li>
 *   <li>{@code user} â€?database user (or {@code jdbc.user})</li>
 *   <li>{@code password} â€?database password (or {@code jdbc.password})</li>
 * </ul>
 */
public class JdbcCatalogConfig {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcCatalogConfig.class);

    private final Map<String, String> properties;

    public JdbcCatalogConfig(Properties props) {
        this.properties = new HashMap<>();
        for (String key : props.stringPropertyNames()) {
            this.properties.put(key, props.getProperty(key));
        }
    }

    /**
     * Creates and initializes a new {@link JdbcCatalog} from the configured properties.
     */
    public JdbcCatalog createCatalog() {
        // map convenience keys to Iceberg JdbcCatalog keys
        mapIfPresent("jdbc.url", "uri");
        mapIfPresent("jdbc.user", "user");
        mapIfPresent("jdbc.password", "password");

        String uri = properties.get("uri");
        if (uri == null || uri.isBlank()) {
            throw new IllegalArgumentException("JDBC connection URI is required (set 'uri' or 'jdbc.url')");
        }
        String warehouse = properties.get("warehouse");
        if (warehouse == null || warehouse.isBlank()) {
            throw new IllegalArgumentException("Warehouse location is required (set 'warehouse')");
        }

        JdbcCatalog catalog = new JdbcCatalog();
        catalog.initialize("iceberg_maintenance", properties);
        LOG.info("Initialized JdbcCatalog: uri={}, warehouse={}", uri, warehouse);
        return catalog;
    }

    private void mapIfPresent(String fromKey, String toKey) {
        String val = properties.get(fromKey);
        if (val != null && !properties.containsKey(toKey)) {
            properties.put(toKey, val);
        }
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}
