# Author: Max Hayne

### Build stage
FROM gradle:8.1.1-jdk17-alpine AS build

WORKDIR /app

COPY build.gradle /app
COPY src /app/src
COPY libs /app/libs

RUN gradle build
#RUN gradle clean build


### Controller run stage
FROM eclipse-temurin:17.0.8.1_1-jre-alpine AS controller

WORKDIR /app

# Move jar created from build stage to working directory
COPY --from=build /app/build/libs/*.jar /app/dfs.jar

# Copy config folder for 'application.properties' file
COPY config /app/config

ENTRYPOINT ["java", "-cp", "./dfs.jar", "cs555.overlay.node.Controller"]


### ChunkServer run stage
FROM eclipse-temurin:17.0.8.1_1-jre-alpine AS chunkserver

WORKDIR /app

COPY --from=build /app/build/libs/*.jar /app/dfs.jar
COPY config /app/config

ENTRYPOINT ["java", "-cp", "./dfs.jar", "cs555.overlay.node.ChunkServer"]


### Client run stage
FROM eclipse-temurin:17.0.8.1_1-jre-alpine AS client

WORKDIR /app

COPY --from=build /app/build/libs/*.jar /app/dfs.jar
COPY config /app/config

# Copy data files to client for use in testing
COPY data/large.jpg /app/large.jpg
COPY data/medium.pdf /app/medium.pdf
COPY data/small.txt /app/small.txt

ENTRYPOINT ["java", "-cp", "./dfs.jar", "cs555.overlay.node.Client"]
