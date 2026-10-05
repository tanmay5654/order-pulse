FROM maven:3.9.11-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
COPY common common
COPY orders orders
COPY inventory inventory
COPY notifications notifications
ARG SERVICE
RUN mvn -B -pl ${SERVICE} -am package -DskipTests
RUN cp ${SERVICE}/target/${SERVICE}-1.0.0.jar /app/service.jar
FROM eclipse-temurin:17-jre
WORKDIR /app
RUN useradd --system app
COPY --from=build /app/service.jar app.jar
USER app
ENTRYPOINT ["java","-XX:MaxRAMPercentage=65","-jar","app.jar"]
