# syntax=docker/dockerfile:1
# Multi-stage build for docker-compose users; CI/CD uses Jib (see pom.xml).
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml .
COPY migration-common/pom.xml migration-common/pom.xml
COPY jms-to-pubsub-bridge/pom.xml jms-to-pubsub-bridge/pom.xml
COPY pubsub-to-jms-bridge/pom.xml pubsub-to-jms-bridge/pom.xml
COPY reconciler/pom.xml reconciler/pom.xml
RUN mvn -B -ntp -q -pl pubsub-to-jms-bridge -am dependency:go-offline || true
COPY migration-common migration-common
COPY pubsub-to-jms-bridge pubsub-to-jms-bridge
RUN mvn -B -ntp -q -pl pubsub-to-jms-bridge -am -DskipTests package

FROM eclipse-temurin:21-jre
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 1000 app && useradd --system --uid 1000 --gid app --home /app app
WORKDIR /app
COPY --from=build /workspace/pubsub-to-jms-bridge/target/pubsub-to-jms-bridge-*.jar app.jar
USER app
EXPOSE 8091
ENV SERVER_PORT=8091 JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75.0"
HEALTHCHECK --interval=15s --timeout=3s --start-period=40s --retries=3 \
  CMD curl -fsS http://localhost:8091/actuator/health/liveness || exit 1
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
