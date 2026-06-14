# Distributed KV Store

A distributed, fault-tolerant key-value store built from scratch in Java.
It implements the Raft consensus algorithm for leader election and log replication, and uses a Log-Structured Merge-Tree (LSM) storage engine for durable writes. The system runs as a 3-node cluster where one node is elected leader, writes flow through quorum, and data survives node crashes and restarts.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Client (CLI / HTTP)                      │
└───────────────────────────────┬─────────────────────────────────┘
                                │  KV RPC (Protobuf / Netty)
          ┌─────────────────────▼──────────────────────┐
          │               Leader Node                   │
          │  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
          │  │ RaftNode │  │   WAL    │  │Metrics   │  │
          │  │(Election,│  │(durability│  │HTTP :9081│  │
          │  │ Commit)  │  │ on write)│  │          │  │
          │  └────┬─────┘  └──────────┘  └──────────┘  │
          │       │ AppendEntries RPC                    │
          └───────┼─────────────────────────────────────┘
                  │
       ┌──────────┴──────────┐
       ▼                     ▼
  Follower Node         Follower Node
  (Raft :8082)          (Raft :8083)
  (Metrics :9082)       (Metrics :9083)
```

**Modules:**

| Module | Responsibility |
|---|---|
| `raft-core` | Raft state machine — elections, log replication, commit |
| `networking` | Netty-based TCP transport with Protobuf framing |
| `storage-engine` | LSM Tree (MemTable → SSTable), WAL, log compaction |
| `server-node` | Wires everything together; serves KV & metrics |
| `client-java` | CLI client for PUT / GET / DELETE |

---

## How It Works

- **Raft consensus**: Nodes elect a leader via randomized timeouts (150–300 ms). All writes go to the leader, which appends the command to its log and replicates it to followers using AppendEntries RPCs. An entry is committed once a majority acknowledges it.

- **Log replication & safety**: The leader tracks `nextIndex` and `matchIndex` per follower. On conflict, followers return the first index of the conflicting term so the leader can skip back quickly (accelerated backtracking) instead of decrementing one index at a time.

- **Persistence**: Every time `currentTerm` or `votedFor` changes, they are flushed synchronously to `raft_metadata.properties` before any RPC response is sent. This prevents double-voting after a crash.

- **LSM storage engine**: Writes go to an in-memory MemTable backed by a Write-Ahead Log. When the MemTable reaches 1 MB, it is flushed to an immutable SSTable file. Reads check the MemTable first, then SSTables newest-to-oldest.

- **Log compaction (snapshotting)**: When a node has applied 1000 more entries since its last snapshot, it serializes the full state machine state to `snapshot.bin` atomically (write to `.tmp`, then rename) and truncates the in-memory log. Followers that fall too far behind receive the snapshot directly via an InstallSnapshot RPC.

---

## Running a 3-Node Cluster

### Prerequisites
- Java 21+
- Gradle (wrapper included)

### Build
```powershell
.\gradlew.bat :modules:server-node:installDist :modules:client-java:installDist
```

### Start the cluster (3 terminals)

**Node 1:**
```powershell
.\modules\server-node\build\install\server-node\bin\server-node.bat `
  node1 8081 .\cluster-data\node1 `
  "node2:localhost:8082,node3:localhost:8083"
```

**Node 2:**
```powershell
.\modules\server-node\build\install\server-node\bin\server-node.bat `
  node2 8082 .\cluster-data\node2 `
  "node1:localhost:8081,node3:localhost:8083"
```

**Node 3:**
```powershell
.\modules\server-node\build\install\server-node\bin\server-node.bat `
  node3 8083 .\cluster-data\node3 `
  "node1:localhost:8081,node2:localhost:8082"
```

The cluster will elect a leader within ~300 ms. Check the logs for a `BECAME_LEADER` JSON event.

### Write and read data

```powershell
# PUT
.\modules\client-java\build\install\client-java\bin\client-java.bat localhost 8081 put mykey myvalue

# GET
.\modules\client-java\build\install\client-java\bin\client-java.bat localhost 8081 get mykey
```

### Metrics endpoint

Each node exposes a metrics endpoint at `raftPort + 1000`:

```powershell
curl http://localhost:9081/metrics
```

Example response:
```json
{
  "nodeId": "node1",
  "state": "LEADER",
  "term": 1,
  "commitIndex": 5,
  "lastApplied": 5,
  "peers": [
    { "peerId": "node2", "nextIndex": 6, "matchIndex": 5, "reachable": true },
    { "peerId": "node3", "nextIndex": 6, "matchIndex": 5, "reachable": true }
  ]
}
```

### Live dashboard

Open `dashboard.html` in a browser while the cluster is running. It polls all three metrics endpoints every second and renders live state, term, commit index, and peer replication progress.

### Crash recovery test

```powershell
.\crash_test.ps1
```

Starts the cluster, writes 10 keys, hard-kills all nodes, restarts them, and verifies all 10 keys are readable.

---

## Structured Logs

Log output is JSON lines, grep-able across all three node log files:

```
{"ts":"2024-01-01T00:00:01.000Z","node":"node1","term":1,"state":"CANDIDATE","event":"START_ELECTION","detail":"term=1"}
{"ts":"2024-01-01T00:00:01.050Z","node":"node2","term":1,"state":"FOLLOWER","event":"VOTE_GRANTED","detail":"to=node1 term=1"}
{"ts":"2024-01-01T00:00:01.055Z","node":"node1","term":1,"state":"LEADER","event":"BECAME_LEADER","detail":"term=1 peers=2"}
```

Filter all leader elections across node logs:
```powershell
Select-String "BECAME_LEADER" .\cluster-data\node*\*.log
```

---

## Limitations

- **No TLS** — all inter-node and client traffic is plaintext TCP.
- **Single datacenter** — no cross-region replication or geo-awareness.
- **No cluster membership changes** — adding or removing nodes requires a full restart.
- **Snapshot threshold is high** — the default threshold of 1000 applied entries is suitable for testing; production use would need a configurable, byte-based threshold.
- **No read linearizability guarantee** — reads from followers may return stale data. Only reads routed through the leader are consistent.
