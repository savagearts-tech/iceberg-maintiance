package io.github.iceberg.cli;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class TableTaskResultTest {

    @Test
    void successResult() {
        var r = TableTaskResult.success("my_db.my_table", "expire", 1234L);
        assertTrue(r.success());
        assertEquals("my_db.my_table", r.tableName());
        assertEquals("expire", r.command());
        assertEquals(1234L, r.durationMs());
        assertTrue(r.errorMessage().isEmpty());
    }

    @Test
    void failureResult() {
        var r = TableTaskResult.failure("my_db.my_table", "expire", 567L, "Connection refused");
        assertFalse(r.success());
        assertEquals(567L, r.durationMs());
        assertEquals("Connection refused", r.errorMessage());
    }
}
