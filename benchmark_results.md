# Benchmark Results

**Date:** 2026-06-18 21:50:18
**Machine:** Intel(R) Core(TM) 7 150U, 16 GB RAM
**Configuration:** 3-node cluster, all on localhost
**Duration:** 30s per benchmark run
**Value size:** 100 bytes

---

## Steady-State Performance

| Operation | Concurrency | Throughput (ops/sec) | p50 (ms) | p95 (ms) | p99 (ms) | Min (ms) | Max (ms) | Errors |
|-----------|-------------|----------------------|----------|----------|----------|----------|----------|--------|
| PUT | 10 | 194.0 | 30.91 | 76.29 | 191.05 | 8.15 | 2177.29 | 13 |
| PUT | 50 | 224.2 | 95.40 | 905.37 | 2207.67 | 36.70 | 2407.82 | 18 |
| PUT | 100 | 195.6 | 233.58 | 2022.23 | 2420.09 | 9.15 | 5671.79 | 3313 |
| GET | 50 | 18898.3 | 0.38 | 8.26 | 18.51 | 0.06 | 167.78 | 0 |

## Leader Failover

Leader was killed at the 15-second mark of a 30s PUT benchmark at 50 threads.

| Operation | Concurrency | Throughput (ops/sec) | p50 (ms) | p95 (ms) | p99 (ms) | Min (ms) | Max (ms) | Errors |
|-----------|-------------|----------------------|----------|----------|----------|----------|----------|--------|
| PUT* | 50 | 254.0 | 85.73 | 413.82 | 2207.30 | 34.11 | 2681.52 | 2005 |

*\* = leader killed mid-run*

**Steady-state p99:** 2207.67 ms
**Failover p99:** 2207.30 ms

---

> **Disclaimer:** Tested on Intel(R) Core(TM) 7 150U, 16 GB RAM with all 3 nodes running on localhost.
> Numbers measure protocol overhead (Raft consensus + LSM storage), not network
> latency. Real-world performance across a network will be dominated by round-trip
> time rather than these protocol costs.
