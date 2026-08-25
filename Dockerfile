# syntax=docker/dockerfile:1
#
# One Dockerfile builds all three services; MODULE selects which.
# Multi-stage so the shipped image contains a JRE and a jar - not Maven,
# not the JDK, not the source.

# ---------- build ----------
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build

# POMs first, sources second. Docker caches layers, so editing a .java file
# does not invalidate the dependency layer below it and rebuilds stay fast.
COPY pom.xml ./
COPY common-events/pom.xml           common-events/
COPY account-service/pom.xml         account-service/
COPY payment-orchestrator/pom.xml    payment-orchestrator/
COPY payment-gateway/pom.xml         payment-gateway/

# A BuildKit cache mount keeps ~/.m2 across builds without baking it into a
# layer, so the three service images share one dependency download.
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -ntp dependency:go-offline -DskipTests

COPY common-events/src           common-events/src
COPY account-service/src         account-service/src
COPY payment-orchestrator/src    payment-orchestrator/src
COPY payment-gateway/src         payment-gateway/src

ARG MODULE
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -ntp -pl ${MODULE} -am package -DskipTests && \
    cp ${MODULE}/target/*.jar /build/app.jar

# ---------- runtime ----------
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Run unprivileged. A container process that does not need root should not have it.
RUN addgroup -S app && adduser -S -G app app
COPY --from=build --chown=app:app /build/app.jar app.jar
USER app

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
