#!/bin/bash
# use the following example command to run this file and lauch the twitter producer
# bash spawn_twitter_streams.sh 10.0.0.xyz:9092 4 k1

IP_ADDR=$1
NUM_SPAWNS=$2
SESSION=$3
tmux new-session -s $SESSION -n bash -d
for ID in `seq 1 $NUM_SPAWNS`;
do
    echo $ID
    tmux new-window -t $ID
    tmux send-keys -t $SESSION:$ID 'python twitter_producer.py --kafka-ip-addr '"$IP_ADDR"'' C-m
done
