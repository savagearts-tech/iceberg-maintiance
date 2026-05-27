package com.fds.iceberg.cli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Executes Iceberg table maintenance tasks in parallel using a fixed thread pool.
 *
 * <p>Each table is submitted as a {@link CompletableFuture}. Failures are isolated —
 * an exception in one table does not affect others. Progress is reported periodically
 * and a summary is produced on completion.
 */
public class ParallelMaintenanceExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelMaintenanceExecutor.class);

    private final int parallelism;

    /**
     * @param parallelism maximum number of concurrent table tasks
     */
    public ParallelMaintenanceExecutor(int parallelism) {
        if (parallelism < 1) {
            throw new IllegalArgumentException("parallelism must be >= 1, got: " + parallelism);
        }
        this.parallelism = parallelism;
    }

    /**
     * Executes a maintenance task on each table in parallel.
     *
     * @param tables   list of table identifiers to process
     * @param command  the command name (for result recording)
     * @param task     callback that performs the actual maintenance for a single table
     * @return list of per-table results in completion order
     */
    public BatchResult executeAll(
            List<org.apache.iceberg.catalog.TableIdentifier> tables,
            String command,
            Consumer<org.apache.iceberg.catalog.TableIdentifier> task) {

        Instant start = Instant.now();
        int total = tables.size();

        // Collect results in insertion order for stable reporting
        List<TableTaskResult> results = new CopyOnWriteArrayList<>();
        int logInterval = Math.max(1, total / 10);

        ExecutorService executor = Executors.newFixedThreadPool(parallelism);
        registerShutdownHook(executor);

        // Submit each table as a CompletableFuture
        List<CompletableFuture<Void>> futures = tables.stream()
                .map(id -> CompletableFuture.runAsync(() -> {
                    Instant taskStart = Instant.now();
                    String tableName = id.toString();
                    try {
                        task.accept(id);
                        long elapsed = Duration.between(taskStart, Instant.now()).toMillis();
                        results.add(TableTaskResult.success(tableName, command, elapsed));
                    } catch (Exception e) {
                        long elapsed = Duration.between(taskStart, Instant.now()).toMillis();
                        results.add(TableTaskResult.failure(tableName, command, elapsed, e.getMessage()));
                        LOG.warn("Table {} failed: {}", tableName, e.getMessage());
                    }
                }, executor))
                .toList();

        // Wait for all tasks
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        // Shutdown executor
        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        long wallClockMs = Duration.between(start, Instant.now()).toMillis();
        return new BatchResult(results, wallClockMs, total);
    }

    private void registerShutdownHook(ExecutorService executor) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown requested, waiting up to 30s for running tasks...");
            executor.shutdown();
            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }));
    }

    /**
     * Container for batch execution results.
     */
    public record BatchResult(List<TableTaskResult> results, long wallClockMs, int totalTables) {
        public int succeeded() {
            return (int) results.stream().filter(TableTaskResult::success).count();
        }

        public int failed() {
            return (int) results.stream().filter(r -> !r.success()).count();
        }

        public String summary() {
            return String.format(
                    """
                            Batch complete: %d/%d tables processed (%d failed) in %.1f s
                            """,
                    results.size(), totalTables, failed(), wallClockMs / 1000.0);
        }
    }
}
