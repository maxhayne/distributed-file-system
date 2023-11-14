#!/usr/bin/env bash

# RUN USING REPLICATION:
sed -i '' -e 's/storageType = erasure/storageType = replication/' ../config/application.properties
cat ../config/application.properties

# Build and start the project
docker compose build
docker compose up -d --force-recreate

sleep 4 # wait for containers to start up

# Start collecting stats, capture PID
: > docker-replication-stats
docker stats --no-trunc --format=json > docker-replication-stats &
STATS_PID=$!

sleep 3 

# Store available files
echo "put large.jpg" | socat EXEC:"docker attach dfs-client-1",pty STDIO
echo "put medium.pdf" | socat EXEC:"docker attach dfs-client-1",pty STDIO
echo "put small.txt" | socat EXEC:"docker attach dfs-client-1",pty STDIO

sleep 10 # wait for storage

echo "files" | socat EXEC:"docker attach dfs-client-1",pty STDIO
echo "get 0 1 2" | socat EXEC:"docker attach dfs-client-1",pty STDIO

sleep 7 # wait for retrieval

kill $STATS_PID

# Shutdown the session
docker compose down

sleep 3

# WITH ERASURE CODING:
sed -i '' -e 's/storageType = replication/storageType = erasure/' ../config/application.properties
cat ../config/application.properties

# Build and start the project
docker compose build
docker compose up -d --force-recreate

sleep 4 # wait for containers to start up

# Start collecting stats, capture PID
: > docker-erasure-stats
docker stats --no-trunc --format=json > docker-erasure-stats &
STATS_PID=$!

sleep 3 

# Store available files
echo "put large.jpg" | socat EXEC:"docker attach dfs-client-1",pty STDIO
echo "put medium.pdf" | socat EXEC:"docker attach dfs-client-1",pty STDIO
echo "put small.txt" | socat EXEC:"docker attach dfs-client-1",pty STDIO

sleep 10 # wait for storage

echo "files" | socat EXEC:"docker attach dfs-client-1",pty STDIO
echo "get 0 1 2" | socat EXEC:"docker attach dfs-client-1",pty STDIO

sleep 7 # wait for retrieval

kill $STATS_PID

# Shutdown the session
docker compose down

# Generate plots using the statistics
python3 plot-stats.py