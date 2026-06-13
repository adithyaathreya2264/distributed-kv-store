#!/bin/bash
# crash_test.sh — Verify data survives a hard kill and restart of all 3 nodes.
#
# Usage:
#   ./crash_test.sh
#
# What it does:
#   1. Builds the project
#   2. Starts a 3-node cluster
#   3. Writes 10 keys through the leader
#   4. Hard kills all nodes (kill -9)
#   5. Restarts all nodes
#   6. Reads back all 10 keys and verifies they exist

set -e

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="$PROJECT_ROOT/cluster-data"
SERVER_BIN="$PROJECT_ROOT/modules/server-node/build/install/server-node/bin/server-node"
CLIENT_CMD="./gradlew -q :modules:client-java:run --args"

echo "=== Building project ==="
cd "$PROJECT_ROOT"
./gradlew :modules:server-node:installDist :modules:client-java:installDist --quiet

echo "=== Cleaning data directories ==="
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR/node1" "$DATA_DIR/node2" "$DATA_DIR/node3"

start_cluster() {
    echo "=== Starting cluster ==="
    $SERVER_BIN node1 8081 "$DATA_DIR/node1" "node2:localhost:8082,node3:localhost:8083" &
    echo $! > "$DATA_DIR/node1.pid"

    $SERVER_BIN node2 8082 "$DATA_DIR/node2" "node1:localhost:8081,node3:localhost:8083" &
    echo $! > "$DATA_DIR/node2.pid"

    $SERVER_BIN node3 8083 "$DATA_DIR/node3" "node1:localhost:8081,node2:localhost:8082" &
    echo $! > "$DATA_DIR/node3.pid"

    echo "Waiting for cluster to elect a leader..."
    sleep 3
}

kill_cluster() {
    echo "=== Hard killing all nodes ==="
    for node in node1 node2 node3; do
        pid_file="$DATA_DIR/$node.pid"
        if [ -f "$pid_file" ]; then
            pid=$(cat "$pid_file")
            if kill -0 "$pid" 2>/dev/null; then
                kill -9 "$pid" 2>/dev/null || true
                echo "  Killed $node (PID=$pid)"
            fi
            rm -f "$pid_file"
        fi
    done
}

# ---- Phase 1: Start, Write, Kill ----
start_cluster

echo ""
echo "=== Writing 10 keys ==="
FAILED=0
for i in $(seq 1 10); do
    echo "  PUT key$i = value$i"
    $CLIENT_CMD "localhost 8081 put key$i value$i" 2>/dev/null || {
        # Try other nodes if leader is not node1
        $CLIENT_CMD "localhost 8082 put key$i value$i" 2>/dev/null || \
        $CLIENT_CMD "localhost 8083 put key$i value$i" 2>/dev/null || {
            echo "  FAILED to write key$i"
            FAILED=$((FAILED + 1))
        }
    }
done

echo ""
kill_cluster

echo ""
echo "=== Waiting 2 seconds before restart ==="
sleep 2

# ---- Phase 2: Restart and Verify ----
start_cluster

echo ""
echo "=== Reading back 10 keys ==="
PASS=0
FAIL=0
for i in $(seq 1 10); do
    RESULT=""
    # Try all nodes until we get a response (new leader may be any node)
    for port in 8081 8082 8083; do
        RESULT=$($CLIENT_CMD "localhost $port get key$i" 2>/dev/null) && break
    done

    if echo "$RESULT" | grep -q "value$i"; then
        echo "  key$i = value$i ✓"
        PASS=$((PASS + 1))
    else
        echo "  key$i MISSING or WRONG ✗ (got: $RESULT)"
        FAIL=$((FAIL + 1))
    fi
done

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="

# Cleanup
kill_cluster

if [ "$FAIL" -eq 0 ]; then
    echo "✓ CRASH RECOVERY TEST PASSED"
    exit 0
else
    echo "✗ CRASH RECOVERY TEST FAILED"
    exit 1
fi
