FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B package -DskipTests

FROM eclipse-temurin:17-jre
COPY --from=build /build/target/replay-producer.jar /app/replay-producer.jar
ENTRYPOINT ["java", "-jar", "/app/replay-producer.jar"]
CMD ["replay"]
