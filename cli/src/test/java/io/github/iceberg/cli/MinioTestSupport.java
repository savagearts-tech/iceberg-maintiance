package io.github.iceberg.cli;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.LegacyMd5Plugin;

import java.net.URI;

/**
 * Shared MinIO connectivity for E2E tests (localhost:9000, default credentials).
 */
final class MinioTestSupport {

    static final String ENDPOINT = System.getProperty("minio.endpoint", "http://localhost:9000");
    static final String ACCESS_KEY = System.getProperty("minio.accessKey", "minioadmin");
    static final String SECRET_KEY = System.getProperty("minio.secretKey", "minioadmin");
    static final String BUCKET = System.getProperty("minio.bucket", "iceberg-e2e");

    private MinioTestSupport() {}

    static boolean isReachable() {
        try (S3Client probe = buildS3Client(true)) {
            probe.headBucket(HeadBucketRequest.builder().bucket(BUCKET).build());
            return true;
        } catch (NoSuchBucketException e) {
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    static S3Client buildS3Client(boolean legacyMd5) {
        var builder = S3Client.builder()
                .endpointOverride(URI.create(ENDPOINT))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .forcePathStyle(true)
                .httpClient(UrlConnectionHttpClient.builder().build())
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED);
        if (legacyMd5) {
            builder.addPlugin(LegacyMd5Plugin.create());
        }
        return builder.build();
    }

    static void ensureBucket(S3Client s3) {
        try {
            s3.headBucket(HeadBucketRequest.builder().bucket(BUCKET).build());
        } catch (NoSuchBucketException e) {
            s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        }
    }
}
