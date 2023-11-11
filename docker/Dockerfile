# Author: Max Hayne

### Build stage
FROM gradle:8.1.1-jdk17-alpine AS build

RUN mkdir /app && chown -R gradle:gradle /app

WORKDIR /app

USER gradle

COPY --chown=gradle:gradle build.gradle /app
COPY --chown=gradle:gradle src /app/src
COPY --chown=gradle:gradle libs /app/libs

RUN gradle build


### Controller run stage
FROM eclipse-temurin:17.0.8.1_1-jre-alpine AS controller

# Create non-root user to be used for application
RUN addgroup -g 1001 -S controller && adduser -u 1001 -S controller -G controller

RUN mkdir /app && chown -R controller:controller /app

WORKDIR /app

# Move jar created from build stage to working directory
COPY --from=build --chown=controller:controller /app/build/libs/*.jar /app/dfs.jar

# Copy config folder for 'application.properties' file
COPY --chown=controller:controller config /app/config

# Switch to non-root user
USER controller

ENTRYPOINT ["java", "-cp", "./dfs.jar", "cs555.overlay.node.Controller"]


### ChunkServer run stage
FROM eclipse-temurin:17.0.8.1_1-jre-alpine AS chunkserver

# Create non-root user to be used for application
RUN addgroup -g 1001 -S server && adduser -u 1001 -S server -G server

RUN mkdir /app && chown -R server:server /app

WORKDIR /app

COPY --from=build --chown=server:server /app/build/libs/*.jar /app/dfs.jar
COPY --chown=server:server config /app/config

# Switch to non-root user
USER server

ENTRYPOINT ["java", "-cp", "./dfs.jar", "cs555.overlay.node.ChunkServer"]


### Client run stage
FROM eclipse-temurin:17.0.8.1_1-jre-alpine AS client

# Create non-root user to be used for application
RUN addgroup -g 1001 -S client && adduser -u 1001 -S client -G client

RUN mkdir /app && chown -R client:client /app

WORKDIR /app

COPY --from=build --chown=client:client /app/build/libs/*.jar /app/dfs.jar
COPY --chown=client:client config /app/config

# Copy data files to client for use in testing
COPY --chown=client:client data/large.jpg /app/large.jpg
COPY --chown=client:client data/medium.pdf /app/medium.pdf
COPY --chown=client:client data/small.txt /app/small.txt

# Switch to non-root user
USER client

ENTRYPOINT ["java", "-cp", "./dfs.jar", "cs555.overlay.node.Client"]
