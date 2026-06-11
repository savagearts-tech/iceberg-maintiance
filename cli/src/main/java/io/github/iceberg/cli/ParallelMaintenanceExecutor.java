package io.github.iceberg.cli;

import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Executes Iceberg table maintenance tasks in parallel using a fixed thread pool.
 *
 * <p>Each table is submitted as a {@link CompletableFuture}. Failures are isolated -
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
            List<TableIdentifier> tables,
            String command,
            Consumer<TableIdentifier> task) {

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
                    } catch (Error e) {
                        long elapsed = Duration.between(taskStart, Instant.now()).toMillis();
                        results.add(TableTaskResult.failure(tableName, command, elapsed, e.getMessage()));
                        LOG.error("Table {} encountered fatal error: {}", tableName, e.getMessage(), e);
                        throw e;  // re-throw Error to propagate, but at least we recorded it
                    }
                }, executor))
                .toList();

        // Wait for all tasks and ensure executor is always shut down
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } finally {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        long wallClockMs = Duration.between(start, Instant.now()).toMillis();
        return new BatchResult(results, wallClockMs, total);
    }

    /**
     * Executes a maintenance task on each table from a lazy stream, with backpressure.
     *
     * <p>Tables are pulled from the stream and submitted to the thread pool one at a time,
     * but at most {@code batchSize} tasks are in-flight at any moment. This bounds memory
     * usage regardless of catalog size.
     *
     * @param tableStream lazy stream of table identifiers
     * @param command     the command name (for result recording)
     * @param task        callback that performs the actual maintenance for a single table
     * @param batchSize   maximum number of in-flight tasks (backpressure limit)
     * @return batch result with per-table outcomes
     */
    public BatchResult executeAll(
            Stream<TableIdentifier> tableStream,
            String command,
            Consumer<TableIdentifier> task,
            int batchSize) {

        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be >= 1, got: " + batchSize);
        }

        Instant start = Instant.now();
        List<TableTaskResult> results = new CopyOnWriteArrayList<>();
        Semaphore semaphore = new Semaphore(batchSize);
        Iterator<TableIdentifier> iterator = tableStream.iterator();

        ExecutorService executor = Executors.newFixedThreadPool(parallelism);
        registerShutdownHook(executor);

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        try {
            while (iterator.hasNext()) {
                TableIdentifier id = iterator.next();
                semaphore.acquire();

                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
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
                    } catch (Error e) {
                        long elapsed = Duration.between(taskStart, Instant.now()).toMillis();
                        results.add(TableTaskResult.failure(tableName, command, elapsed, e.getMessage()));
                        LOG.error("Table {} encountered fatal error: {}", tableName, e.getMessage(), e);
                        throw e;
                    } finally {
                        semaphore.release();
                    }
                }, executor);

                futures.add(future);
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Batch execution interrupted");
        } finally {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        int total = results.size();
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
