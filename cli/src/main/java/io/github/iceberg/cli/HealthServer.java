package io.github.iceberg.cli;

import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/**
 * Lightweight health check HTTP server for Sidecar deployment pattern.
 * Listens on port 8080 and responds with 200 OK on /health.
 */
public class HealthServer {

    private static final Logger LOG = LoggerFactory.getLogger(HealthServer.class);

    private final HttpServer server;

    public HealthServer() {
        this(8080);
    }

    public HealthServer(int port) {
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/health", exchange -> {
                String response = "{\"status\":\"UP\"}";
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, response.length());
                exchange.getResponseBody().write(response.getBytes());
                exchange.close();
            });
            server.setExecutor(Executors.newSingleThreadExecutor());
            LOG.info("Health server listening on port {}", port);
        } catch (IOException e) {
            throw new RuntimeException("Failed to start health server", e);
        }
    }

    public void start() {
        server.start();
    }

    public void stop() {
        server.stop(0);
    }
}
