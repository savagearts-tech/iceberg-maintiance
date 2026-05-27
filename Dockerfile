FROM eclipse-temurin:21-jre-alpine AS base
WORKDIR /app

FROM base AS build
COPY cli/target/*.jar /app/app.jar

FROM base
COPY --from=build /app/app.jar /app/iceberg-maintenance.jar

# health check endpoint via HTTP on port 8080
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD wget -qO- http://localhost:8080/health || exit 1

ENTRYPOINT ["java", "-jar", "/app/iceberg-maintenance.jar"]
