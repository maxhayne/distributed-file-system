#!/bin/bash
#
# run script for MacOSX.  Run in the top level directory of project.
# Controller will start in the terminal, and a new window will be opened.
# This new window will spawn MULTI Chunk Servers.
#

# Originally written by Jason Stock, Graduate Student at CSU in Computer Science
# I've taken the base file and modified it so that it works with my code

MULTI="1 2 3 4 5 6 7 8 9 10 11"

DIR="$( cd "$( dirname "$0" )" && pwd )"
JAR_PATH="$DIR/conf/:$DIR/build/libs/distributed-file-system.jar"
COMPILE="$( ps -ef | grep [c]s555.overlay.node.Controller )"

SCRIPT="java -cp $JAR_PATH cs555.overlay.node.ChunkServer"

# Needed to add 'delay 1' to osascript, as if the ChunkServer is started
# to quickly after the tab is created, the tab will start the app from the
# home directory, and not from the directory the new window was spawned to.
function new_tab() {
    osascript \
        -e "tell application \"Terminal\"" \
        -e "tell application \"System Events\" to keystroke \"t\" using {command down}" \
        -e "delay 0.5" \
        -e "do script \"$SCRIPT $1;\" in front window" \
        -e "end tell" > /dev/null
}

if [[ -z "$COMPILE" ]]
then
LINES=`find . -name "*.java" -print | xargs wc -l | grep "total" | awk '{$1=$1};1'`
    echo Project has "$LINES" lines
    gradle clean
    gradle build
    open -a Terminal .
    open -a Terminal .
    java -cp $JAR_PATH cs555.overlay.node.Controller;
elif [[ $1 = "c" ]]
then
    java -cp $JAR_PATH cs555.overlay.node.Client;
else
    if [[ -n "$MULTI" ]]
    then
        for tab in `echo $MULTI`
        do
            new_tab
        done
    fi
fi
