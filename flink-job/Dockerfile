FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B package -DskipTests

FROM flink:2.3.0-java17
# The connector logs every polled message at INFO; keep it quiet so our counters are readable.
RUN printf '\nlogger.iggy.name = org.apache.iggy\nlogger.iggy.level = WARN\n' >> /opt/flink/conf/log4j-console.properties
COPY --from=build /build/target/flink-job.jar /opt/flink/usrlib/flink-job.jar
