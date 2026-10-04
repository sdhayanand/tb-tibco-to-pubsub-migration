# syntax=docker/dockerfile:1
# Multi-stage build for docker-compose users; CI/CD uses Jib (see pom.xml).
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml .
COPY migration-common/pom.xml migration-common/pom.xml
COPY jms-to-pubsub-bridge/pom.xml jms-to-pubsub-bridge/pom.xml
COPY pubsub-to-jms-bridge/pom.xml pubsub-to-jms-bridge/pom.xml
COPY reconciler/pom.xml reconciler/pom.xml
RUN mvn -B -ntp -q -pl reconciler -am dependency:go-offline || true
COPY migration-common migration-common
COPY reconciler reconciler
RUN mvn -B -ntp -q -pl reconciler -am -DskipTests package

FROM eclipse-temurin:21-jre
RUN groupadd --system --gid 1000 app && useradd --system --uid 1000 --gid app --home /app app
WORKDIR /app
COPY --from=build /workspace/reconciler/target/reconciler-*.jar app.jar
USER app
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75.0"
# batch job: no port, no HEALTHCHECK; arguments are appended by the caller, e.g.
#   docker run ... tb-reconciler --from=2026-10-03 --to=2026-10-04 --legacyFile=/data/legacy.csv --pubsubFile=/data/events.jsonl
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
