# Distributed Key-Value Store

A strong-consistent, distributed Key-Value Store implemented in Java. It mimics systems like etcd or TiKV, featuring a custom Raft consensus algorithm, an LSM-Tree based storage engine, and a scalable networking layer.

## Features

- **Consensus Algorithm**: Custom implementation of the **Raft** consensus protocol (Leader Election, Log Replication).
- **Storage Engine**: **LSM-Tree** (Log-Structured Merge Tree) with MemTable, SSTables (Sorted String Tables), and WAL (Write-Ahead Log) for high write throughput and durability.
- **Networking**: Asynchronous, non-blocking RPC layer built on **Netty** and **Protocol Buffers**.
- **Resilience**: Automatic leader failover and crash recovery via WAL replay.
- **Client**: Smart Java client with automatic retry policies and leader discovery.

## Project Structure

The project is modularized using Gradle:

| Module | Description |
|--------|-------------|
| `modules/raft-core` | Core Raft logic (Election, Log management, State transitions). |
| `modules/storage-engine` | Persistent storage (LSM-Tree, SSTable writers/readers, WAL). |
| `modules/networking` | Netty server/client infrastructure and Protobuf RPC handlers. |
| `modules/server-node` | Main server application integrating Raft, Storage, and Networking. |
| `modules/client-java` | Client library and interactive CLI for accessing the store. |
| `proto/` | Shared Protocol Buffer definitions (`kv.proto`, `raft.proto`). |

## Prerequisites

- **Java 21+**
- **Gradle** (Wrapper included)

## Build & Run

### 1. Build the Project
Compile all modules and generate Protobuf sources.
```bash
# Windows
.\gradlew.bat build -x test

# Mac/Linux
./gradlew build -x test
```

### 2. Start a Server Node
Run a single-node cluster (it will auto-elect itself as Leader).
*Usage: `run --args="<nodeId> <port> <dataDir>"`*

```bash
# Windows
.\gradlew.bat :modules:server-node:run --args="node1 8081 data/node1" --console=plain

# Mac/Linux
./gradlew :modules:server-node:run --args="node1 8081 data/node1" --console=plain
```

### 3. Start the Interactive Client
Open a new terminal to run the client shell.
*Usage: `run --args="<seedNodeAddress>"`*

```bash
# Windows
.\gradlew.bat :modules:client-java:run --args="localhost:8081" -q --console=plain

# Mac/Linux
./gradlew :modules:client-java:run --args="localhost:8081" -q --console=plain
```

## Usage Guide

Once inside the Client Shell:

**Put a Key-Value pair:**
```bash
> put user:100 {"name": "Alice", "role": "admin"}
OK
```

**Get a Value:**
```bash
> get user:100
Value: {"name": "Alice", "role": "admin"}
```

**Delete a Key:**
```bash
> delete user:100
OK
```

## Testing

Run unit and integration tests across all modules:
```bash
.\gradlew.bat test
```

## 🔮 Future Improvements

- [ ] Multi-node cluster configuration support in `DKVServer` CLI.
- [ ] Log Compaction (Snapshotting) to prevent infinite log growth.
- [ ] Implement full compaction strategies (K-Way Merge) for SSTables.
- [ ] Membership changes (Add/Remove nodes dynamically).

---
