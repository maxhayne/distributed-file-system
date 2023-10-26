#!/bin/bash
set -e # Exit script if any command fails
#
# Assuming tmux is installed, this script will create a new session
# called 'dfs', and fill the session with a Controller, ChunkServers,
# and a Client as windows.
#
# A folder called 'servers' will be created in the folder from which
# this script is being run, which will itself be populated with as
# many folders as there are ChunkServers. ChunkServer1 will store its
# data files in folder ./servers/server1 and so on.
#
# Modify the 'MULTI' variable to contain as many servers as you'd like.
#
# This assumes that you have some knowledge of tmux.
#

MULTI="1 2 3 4 5 6 7 8 9"

DIR="$( cd "$( dirname "$0" )" && pwd )"
JAR_PATH="$DIR/conf/:$DIR/build/libs/distributed-file-system.jar"

START_CONTROLLER="java -cp $JAR_PATH cs555.overlay.node.Controller"
START_CHUNKSERVER="java -cp $JAR_PATH cs555.overlay.node.ChunkServer"
START_CLIENT="java -cp $JAR_PATH cs555.overlay.node.Client"

# Print lines and build project
LINES=`find . -name "*.java" -print | xargs wc -l | grep "total" | awk '{$1=$1};1'`
echo Project has "$LINES" lines
gradle clean
gradle build

# Create dfs session and all necessary windows
tmux new-session -d -s dfs
tmux rename-window -t dfs:0 "Controller"
for server in $MULTI
do
	tmux new-window -d -n ChunkServer$server
done
tmux new-window -d -n Client

# Send start commands to all windows and execute
echo ""
echo "Attempting to start Controller..."
tmux send-keys -t dfs:Controller "$START_CONTROLLER" C-m
sleep 3
echo "Attempting to start ChunkServers..."
for server in $MULTI
do
	tmux send-keys -t dfs:ChunkServer$server "$START_CHUNKSERVER" C-m
	sleep 0.2
done
echo "Attempting to start Client..."
tmux send-keys -t dfs:Client "$START_CLIENT" C-m

# Minimal instructions
echo "tmux session 'dfs' has been created with Controller, ChunkServers, and Client windows"
echo ""
echo "useful tips:"
echo "    list windows: tmux lsw"
echo "    attach session: tmux attach -t dfs"
echo "    kill session: tmux kill-session -t dfs"
echo "    use 'Ctrl-B n' or 'Ctrl-B p' to go to next, or previous window"
echo "    use 'Ctrl-B d' to detach from session and return to shell"
echo ""
