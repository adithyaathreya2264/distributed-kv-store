#!/bin/bash
# cluster.sh — Start a 3-node DKV cluster locally.
#
# Usage:
#   ./cluster.sh start    — build and launch 3 nodes
#   ./cluster.sh stop     — kill all running nodes
#   ./cluster.sh restart  — stop then start
#
# After starting, send requests with the existing client:
#   ./gradlew :modules:client-java:run --args="localhost 8081 put mykey myvalue"
#   ./gradlew :modules:client-java:run --args="localhost 8081 get mykey"

set -e

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="$PROJECT_ROOT/cluster-data"

start_cluster() {
    echo "=== Building project ==="
    cd "$PROJECT_ROOT"
    ./gradlew :modules:server-node:installDist --quiet

    echo "=== Cleaning data directories ==="
    rm -rf "$DATA_DIR"
    mkdir -p "$DATA_DIR/node1" "$DATA_DIR/node2" "$DATA_DIR/node3"

    SERVER_BIN="$PROJECT_ROOT/modules/server-node/build/install/server-node/bin/server-node"

    echo "=== Starting node1 on port 8081 ==="
    $SERVER_BIN node1 8081 "$DATA_DIR/node1" "node2:localhost:8082,node3:localhost:8083" &
    echo $! > "$DATA_DIR/node1.pid"

    echo "=== Starting node2 on port 8082 ==="
    $SERVER_BIN node2 8082 "$DATA_DIR/node2" "node1:localhost:8081,node3:localhost:8083" &
    echo $! > "$DATA_DIR/node2.pid"

    echo "=== Starting node3 on port 8083 ==="
    $SERVER_BIN node3 8083 "$DATA_DIR/node3" "node1:localhost:8081,node2:localhost:8082" &
    echo $! > "$DATA_DIR/node3.pid"

    echo ""
    echo "=== Cluster started ==="
    echo "  node1 PID=$(cat "$DATA_DIR/node1.pid") — port 8081"
    echo "  node2 PID=$(cat "$DATA_DIR/node2.pid") — port 8082"
    echo "  node3 PID=$(cat "$DATA_DIR/node3.pid") — port 8083"
    echo ""
    echo "Send a PUT request:"
    echo "  ./gradlew :modules:client-java:run --args=\"localhost 8081 put hello world\""
    echo ""
    echo "Stop the cluster:"
    echo "  ./cluster.sh stop"
}

stop_cluster() {
    echo "=== Stopping cluster ==="
    for node in node1 node2 node3; do
        pid_file="$DATA_DIR/$node.pid"
        if [ -f "$pid_file" ]; then
            pid=$(cat "$pid_file")
            if kill -0 "$pid" 2>/dev/null; then
                echo "  Killing $node (PID=$pid)"
                kill "$pid" 2>/dev/null || true
            else
                echo "  $node (PID=$pid) already stopped"
            fi
            rm -f "$pid_file"
        fi
    done
    echo "=== Cluster stopped ==="
}

case "${1:-start}" in
    start)
        start_cluster
        ;;
    stop)
        stop_cluster
        ;;
    restart)
        stop_cluster
        sleep 1
        start_cluster
        ;;
    *)
        echo "Usage: $0 {start|stop|restart}"
        exit 1
        ;;
esac
