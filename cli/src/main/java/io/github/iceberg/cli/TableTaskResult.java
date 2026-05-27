package io.github.iceberg.cli;

/**
 * Result of a single table maintenance task in batch mode.
 *
 * @param tableName    fully qualified Iceberg table name
 * @param command      the maintenance command executed (expire, scan-orphans, cleanup)
 * @param durationMs   wall-clock duration in milliseconds
 * @param success      {@code true} if the task completed without exception
 * @param errorMessage error detail when success is {@code false}; empty otherwise
 */
public record TableTaskResult(
        String tableName,
        String command,
        long durationMs,
        boolean success,
        String errorMessage
) {
    public static TableTaskResult success(String tableName, String command, long durationMs) {
        return new TableTaskResult(tableName, command, durationMs, true, "");
    }

    public static TableTaskResult failure(String tableName, String command, long durationMs, String errorMessage) {
        return new TableTaskResult(tableName, command, durationMs, false, errorMessage);
    }
}
