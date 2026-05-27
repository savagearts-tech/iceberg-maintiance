package com.fds.iceberg.common;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class TableIdentifierParserTest {

    @Test
    void databaseTable() {
        var id = TableIdentifierParser.parse("my_db.my_table");
        assertEquals("my_table", id.name());
        assertArrayEquals(new String[]{"my_db"}, id.namespace().levels());
    }

    @Test
    void catalogDatabaseTable() {
        var id = TableIdentifierParser.parse("my_catalog.my_db.my_table");
        assertEquals("my_table", id.name());
        assertArrayEquals(new String[]{"my_catalog", "my_db"}, id.namespace().levels());
    }

    @Test
    void tableName() {
        assertEquals("my_table", TableIdentifierParser.tableName("db.my_table"));
    }

    @Test
    void namespaceParts() {
        assertEquals(2, TableIdentifierParser.namespaceParts("a.b.t").size());
    }

    @Test
    void tooShortThrows() {
        assertThrows(IllegalArgumentException.class, () -> TableIdentifierParser.parse("just_table"));
    }

    @Test
    void blankThrows() {
        assertThrows(IllegalArgumentException.class, () -> TableIdentifierParser.parse("  "));
    }

    @Test
    void nullThrows() {
        assertThrows(IllegalArgumentException.class, () -> TableIdentifierParser.parse(null));
    }
}
