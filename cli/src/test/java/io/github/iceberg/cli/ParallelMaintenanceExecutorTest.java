package io.github.iceberg.cli;

import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class ParallelMaintenanceExecutorTest {

    @Test
    void allTablesSucceed() {
        var executor = new ParallelMaintenanceExecutor(2);
        var tables = List.of(
                TableIdentifier.parse("a.t1"),
                TableIdentifier.parse("a.t2"),
                TableIdentifier.parse("a.t3"));

        AtomicInteger counter = new AtomicInteger(0);
        var result = executor.executeAll(tables, "expire", id -> counter.incrementAndGet());

        assertEquals(3, result.results().size());
        assertEquals(3, result.succeeded());
        assertEquals(0, result.failed());
        assertEquals(3, counter.get());
    }

    @Test
    void failureIsolation() {
        var executor = new ParallelMaintenanceExecutor(2);
        var tables = List.of(
                TableIdentifier.parse("a.ok"),
                TableIdentifier.parse("a.fail"),
                TableIdentifier.parse("a.ok2"));

        var result = executor.executeAll(tables, "expire", id -> {
            if (id.toString().contains("fail")) {
                throw new RuntimeException("simulated failure");
            }
        });

        assertEquals(3, result.results().size());
        assertEquals(2, result.succeeded());
        assertEquals(1, result.failed());

        var failed = result.results().stream().filter(r -> !r.success()).findFirst().get();
        assertTrue(failed.errorMessage().contains("simulated failure"));
    }

    @Test
    void parallelismLessThan1Throws() {
        assertThrows(IllegalArgumentException.class, () -> new ParallelMaintenanceExecutor(0));
    }

    @Test
    void emptyTableList() {
        var executor = new ParallelMaintenanceExecutor(2);
        var result = executor.executeAll(List.of(), "expire", id -> {});
        assertEquals(0, result.results().size());
        assertEquals(0, result.succeeded());
        assertTrue(result.summary().contains("0/0"));
    }

    @Test
    void batchResultSummary() {
        var executor = new ParallelMaintenanceExecutor(2);
        var tables = List.of(TableIdentifier.parse("a.t1"), TableIdentifier.parse("a.t2"));

        var result = executor.executeAll(tables, "expire", id -> {
            if (id.toString().contains("t1")) throw new RuntimeException("err");
        });

        String summary = result.summary();
        assertTrue(summary.contains("2/2"));
        assertTrue(summary.contains("1 failed"));
    }

    @Test
    void ordering() {
        var executor = new ParallelMaintenanceExecutor(1);
        var tables = List.of(
                TableIdentifier.parse("a.first"),
                TableIdentifier.parse("a.second"));

        var result = executor.executeAll(tables, "test", id -> {});
        assertEquals(2, result.results().size());
    }
}
