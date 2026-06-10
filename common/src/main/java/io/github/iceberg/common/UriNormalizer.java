package io.github.iceberg.common;

import java.net.URI;
import java.util.regex.Pattern;

/**
 * Normalizes URI protocol headers for consistent path comparison.
 * Converts {@code s3://} and bare paths to {@code s3a://} format.
 */
public class UriNormalizer {

    private static final Pattern S3_PREFIX = Pattern.compile("^s3://", Pattern.CASE_INSENSITIVE);

    private UriNormalizer() {}

    /**
     * Normalizes the given URI to {@code s3a://} format.
     *
     * @param uri the raw URI (may be {@code s3://bucket/key}, {@code s3a://bucket/key}, or {@code bucket/key})
     * @return the normalized URI in {@code s3a://} format, or the original if it has an unrecognized scheme
     */
    public static String normalize(String uri) {
        if (uri == null || uri.isBlank()) {
            return uri;
        }
        String trimmed = uri.trim();
        if (S3_PREFIX.matcher(trimmed).find()) {
            return "s3a://" + trimmed.substring(trimmed.indexOf("://") + 3);
        }
        if (trimmed.startsWith("s3a://")) {
            return trimmed;
        }
        // bare path �?no protocol prefix
        return "s3a://" + trimmed;
    }

    /**
     * Returns {@code true} if both URIs refer to the same path after normalization.
     */
    public static boolean match(String uriA, String uriB) {
        if (uriA == null || uriB == null) {
            return false;
        }
        return normalize(uriA).equals(normalize(uriB));
    }

    public static String extractBucket(String s3Uri) {
        URI uri = URI.create(normalize(s3Uri));
        return uri.getHost();
    }

    public static String extractKey(String s3Uri) {
        URI uri = URI.create(normalize(s3Uri));
        String path = uri.getPath();
        if (path == null) return "";
        return path.startsWith("/") ? path.substring(1) : path;
    }
}
