#!/usr/bin/env bash
# demo.sh - scripted walkthrough for recording a terminal demo.
# Starts a 3-node cluster, writes and reads a key, kills the leader,
# and proves the cluster keeps serving through the failover.
#
# Usage: ./scripts/demo.sh

set -euo pipefail

PEERS="node1=localhost:7001,node2=localhost:7002,node3=localhost:7003"
CLI="./raftkv-cli"
SERVER="./raftkv-server"
DATA_ROOT="/tmp/raftkv-demo"

pause() { sleep "${1:-2}"; }

say() {
    echo ""
    echo "# $1"
    sleep 1
}

cleanup() {
    for id in node1 node2 node3; do
        pid_file="$DATA_ROOT/$id.pid"
        [[ -f "$pid_file" ]] && kill "$(cat "$pid_file")" 2>/dev/null || true
    done
    rm -rf "$DATA_ROOT"
}
trap cleanup EXIT

rm -rf "$DATA_ROOT"
mkdir -p "$DATA_ROOT"

say "starting a 3-node raft cluster"

declare -A ADDRS=([node1]=localhost:7001 [node2]=localhost:7002 [node3]=localhost:7003)
for id in node1 node2 node3; do
    peers=""
    for other in node1 node2 node3; do
        [[ "$other" != "$id" ]] && peers+="${other}=${ADDRS[$other]},"
    done
    mkdir -p "$DATA_ROOT/$id"
    "$SERVER" --id="$id" --listen="${ADDRS[$id]}" --peers="${peers%,}" \
        --data-dir="$DATA_ROOT/$id" > "$DATA_ROOT/$id.log" 2>&1 &
    echo $! > "$DATA_ROOT/$id.pid"
done

pause 3

say "writing a key through the cluster"
$CLI --peers "$PEERS" put hello world

say "reading it back"
$CLI --peers "$PEERS" get hello

leader=""
for id in node1 node2 node3; do
    if tail -1 "$DATA_ROOT/$id.log" 2>/dev/null | grep -q "became leader" \
        || grep -q "became leader" "$DATA_ROOT/$id.log" 2>/dev/null; then
        last_leader=$(grep "became leader" "$DATA_ROOT/$id.log" | tail -1)
        [[ -n "$last_leader" ]] && leader="$id"
    fi
done

say "killing the leader ($leader) to simulate a node failure"
kill "$(cat "$DATA_ROOT/$leader.pid")"
pause 2

say "reading still works, no data lost"
$CLI --peers "$PEERS" get hello

say "and the cluster accepts new writes after electing a new leader"
$CLI --peers "$PEERS" put failover proven
$CLI --peers "$PEERS" get failover

say "done"
