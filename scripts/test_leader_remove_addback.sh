#!/bin/bash

set -o pipefail
set -o errexit

num_nodes=5

get_leader() {
    curl --fail -s -XPOST -H "Content-Type: application/json" \
         127.0.0.1:8080/node/status | \
        jq -r .leader.node
}

wait_leader() {
    local leader

    while true; do
        leader=$(get_leader)
        if [ "$leader" = null ]; then
            sleep 0.1s
            continue
        fi

        echo "$leader" | sed 's/^chronicle_\(.\)@127.0.0.1$/\1/'
        break
    done
}

eject() {
    local port=$(( 8080 + $1 ))
    curl --fail -s -XPOST -H "Content-Type: applycation/json" \
         127.0.0.1:$port/config/removenode \
         -d "\"chronicle_$2@127.0.0.1\"" > /dev/null
}

wipe() {
    local port=$(( 8080 + $1 ))
    curl --fail -s -XPOST -H "Content-Type: applycation/json" \
         127.0.0.1:$port/node/wipe > /dev/null
}

addback() {
    local port=$(( 8080 + $1 ))
    curl --fail -s -XPOST -H "Content-Type: applycation/json" \
         127.0.0.1:$port/config/addnode
         -d "\"chronicle_$2@127.0.0.1\"" > /dev/null
}

get_pid() {
    pid=$(pgrep -f "beam.smp.*chronicle_$1@")
    if [ -z pid ]; then
        false
    else
        echo "$pid"
    fi
}

while true; do
    leader=$(wait_leader)
    echo "Node $leader is the leader"

    other_node=$(( (leader + 1) % $num_nodes ))
    echo "Helper node is $other_node"

    pid=$(get_pid $leader)
    echo "Node $leader pid is $pid"

    kill -SIGSTOP "$pid"
    echo "Stopped node $leader"

    eject $other_node $leader
    echo "Node $leader ejected"

    kill -SIGCONT "$pid"
    echo "Node $leader resumed"

    wipe $leader
    echo "Node $leader wiped"

    addback $other_node $leader
    echo "Node $leader added back"
    echo "========================="
done
