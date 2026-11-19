FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
# Unit tests run here: an image is only built if they pass.
RUN mvn -q -B package

FROM flink:2.3.0-java17
# The connector logs every polled message at INFO; keep it quiet so our counters are readable.
RUN printf '\nlogger.iggy.name = org.apache.iggy\nlogger.iggy.level = WARN\n' >> /opt/flink/conf/log4j-console.properties
COPY --from=build /build/target/flink-job.jar /opt/flink/usrlib/flink-job.jar
# Mount point for the shared checkpoint volume. Created here, owned by flink, so that a new named volume
# inherits the ownership (a fresh volume is otherwise root-owned and not writable for the flink user).
# The base image runs as `flink`, so this step needs root; switch back afterwards.
USER root
RUN mkdir -p /flink-checkpoints && chown flink:flink /flink-checkpoints
USER flink
