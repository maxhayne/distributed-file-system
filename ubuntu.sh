#!/bin/bash
#
# run script for Ubuntu.  Run in the top level directory of project.
# Controller will start in the terminal, and a new window will be opened.
# This new window will spawn MULTI Chunk Servers.
#

# Originally written by Jason Stock, Graduate Student at CSU in Computer Science
# I've taken the base file and modified it so that it works with my code

# When running './ubuntu.sh c', can append another argument 'replication' or 'erasure' to use as an argument for the Client

if [[ -z "$2" ]] || [[ "$2" != "erasure" ]]
then
	TYPE="replication"
else
	TYPE=$2
fi

MULTI="1 2 3 4 5 6 7 8 9"

DIR="$( cd "$( dirname "$0" )" && pwd )"
JAR_PATH="$DIR/conf/:$DIR/build/libs/distributed-file-system.jar"
COMPILE="$( ps -ef | grep [c]s555.overlay.node.Controller )"

SCRIPT="java -cp $JAR_PATH cs555.overlay.node.ChunkServer"

new_tab ()  {
	gnome-terminal --tab -- bash -c "$SCRIPT $1"
}

if [[ -z "$COMPILE" ]]
then
LINES=`find . -name "*.java" -print | xargs wc -l | grep "total" | awk '{$1=$1};1'`
    echo Project has "$LINES" lines
    gradle clean
    gradle build
    gnome-terminal
    gnome-terminal
    java -cp $JAR_PATH cs555.overlay.node.Controller;
elif [[ -n "$1" ]] && [[ $1 == "c" ]]
then
    java -cp $JAR_PATH cs555.overlay.node.Client $TYPE;
else
    if [[ -n "$MULTI" ]]
    then
        mkdir -p "$DIR/serverDirectories"
        for tab in `echo $MULTI`
        do
            mkdir "$DIR/serverDirectories/server$tab"
            new_tab "$DIR/serverDirectories/server$tab"
        done
    fi
    #eval $SCRIPT
fi
