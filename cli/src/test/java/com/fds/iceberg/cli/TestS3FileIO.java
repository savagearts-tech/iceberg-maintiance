package com.fds.iceberg.cli;

import org.apache.iceberg.io.*;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;

/**
 * Minimal S3 FileIO for E2E testing against MinIO.
 * Avoids iceberg-aws's S3FileIO which pulls in heavy transitive deps (STS, KMS).
 */
class TestS3FileIO implements FileIO {

    private final S3Client s3;

    TestS3FileIO(S3Client s3) {
        this.s3 = s3;
    }

    @Override
    public InputFile newInputFile(String path) {
        String bucket = bucketOf(path);
        String key = keyOf(path);
        return new InputFile() {
            @Override
            public long getLength() {
                try {
                    return s3.headObject(HeadObjectRequest.builder()
                            .bucket(bucket).key(key).build()).contentLength();
                } catch (Exception e) {
                    return 0;
                }
            }

            @Override
            public SeekableInputStream newStream() {
                byte[] data = s3.getObjectAsBytes(
                        GetObjectRequest.builder().bucket(bucket).key(key).build()).asByteArray();
                return new SeekableInputStream() {
                    int pos = 0;

                    @Override
                    public long getPos() { return pos; }

                    @Override
                    public void seek(long newPos) { pos = (int) newPos; }

                    @Override
                    public int read() {
                        return pos < data.length ? data[pos++] & 0xFF : -1;
                    }

                    @Override
                    public int read(byte[] b, int off, int len) {
                        if (pos >= data.length) return -1;
                        int n = Math.min(len, data.length - pos);
                        System.arraycopy(data, pos, b, off, n);
                        pos += n;
                        return n;
                    }

                    @Override
                    public void close() {}
                };
            }

            @Override
            public String location() { return path; }

            @Override
            public boolean exists() {
                try {
                    s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
                    return true;
                } catch (Exception e) {
                    return false;
                }
            }
        };
    }

    @Override
    public OutputFile newOutputFile(String path) {
        String bucket = bucketOf(path);
        String key = keyOf(path);
        return new OutputFile() {
            final ByteArrayOutputStream buf = new ByteArrayOutputStream();

            @Override
            public PositionOutputStream create() {
                return new PositionOutputStream() {
                    @Override
                    public long getPos() { return buf.size(); }

                    @Override
                    public void write(int b) { buf.write(b); }

                    @Override
                    public void write(byte[] b, int off, int len) { buf.write(b, off, len); }

                    @Override
                    public void close() {
                        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(),
                                RequestBody.fromBytes(buf.toByteArray()));
                    }
                };
            }

            @Override
            public PositionOutputStream createOrOverwrite() { return create(); }

            @Override
            public String location() { return path; }

            @Override
            public InputFile toInputFile() {
                return newInputFile(location());
            }
        };
    }

    @Override
    public void deleteFile(String path) {
        s3.deleteObject(DeleteObjectRequest.builder()
                .bucket(bucketOf(path)).key(keyOf(path)).build());
    }

    private static String bucketOf(String path) {
        return URI.create(path.replace("s3a://", "s3://")).getHost();
    }

    private static String keyOf(String path) {
        String p = URI.create(path.replace("s3a://", "s3://")).getPath();
        return p.startsWith("/") ? p.substring(1) : p;
    }
}
