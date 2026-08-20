#!/usr/bin/env bash
# run_cluster.sh - starts a 3-node raftkv cluster locally for manual testing.
# Each node gets its own data directory under /tmp/raftkv/.
#
# Usage:
#   ./scripts/run_cluster.sh          # start all 3 nodes
#   ./scripts/run_cluster.sh stop     # stop all 3 nodes
#
# Once running, use the CLI:
#   ./raftkv-cli --peers node1=localhost:7001,node2=localhost:7002,node3=localhost:7003 put mykey hello
#   ./raftkv-cli --peers node1=localhost:7001,node2=localhost:7002,node3=localhost:7003 get mykey

set -euo pipefail

BINARY="./raftkv-server"
DATA_ROOT="/tmp/raftkv"
PEERS="node1=localhost:7001,node2=localhost:7002,node3=localhost:7003"

declare -A NODES=(
    [node1]="localhost:7001"
    [node2]="localhost:7002"
    [node3]="localhost:7003"
)

start_node() {
    local id=$1
    local addr=${NODES[$id]}
    local peers=""

    for other_id in "${!NODES[@]}"; do
        if [[ "$other_id" != "$id" ]]; then
            peers+="${other_id}=${NODES[$other_id]},"
        fi
    done
    peers="${peers%,}"  # trim trailing comma

    local data_dir="$DATA_ROOT/$id"
    mkdir -p "$data_dir"

    echo "Starting $id on $addr..."
    "$BINARY" \
        --id="$id" \
        --listen="$addr" \
        --peers="$peers" \
        --data-dir="$data_dir" \
        > "$DATA_ROOT/$id.log" 2>&1 &
    echo $! > "$DATA_ROOT/$id.pid"
}

stop_all() {
    echo "Stopping all nodes..."
    for id in "${!NODES[@]}"; do
        local pid_file="$DATA_ROOT/$id.pid"
        if [[ -f "$pid_file" ]]; then
            local pid
            pid=$(cat "$pid_file")
            if kill -0 "$pid" 2>/dev/null; then
                kill "$pid"
                echo "Stopped $id (pid $pid)"
            fi
            rm -f "$pid_file"
        fi
    done
}

case "${1:-start}" in
    start)
        mkdir -p "$DATA_ROOT"
        for id in "${!NODES[@]}"; do
            start_node "$id"
        done
        echo ""
        echo "Cluster started. Waiting for leader election..."
        sleep 2
        echo ""
        echo "Try it:"
        echo "  ./raftkv-cli --peers $PEERS put hello world"
        echo "  ./raftkv-cli --peers $PEERS get hello"
        echo ""
        echo "Logs: tail -f $DATA_ROOT/node1.log"
        echo "Stop: $0 stop"
        ;;
    stop)
        stop_all
        ;;
    *)
        echo "Usage: $0 [start|stop]"
        exit 1
        ;;
esac
