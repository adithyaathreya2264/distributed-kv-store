# Benchmark Results

**Date:** 2026-08-29 12:10:14
**Machine:** Intel(R) Core(TM) 7 150U, 16 GB RAM
**Configuration:** 3-node cluster, all on localhost
**Duration:** 30s per benchmark run
**Value size:** 100 bytes

---

## Steady-State Performance

| Operation | Concurrency | Throughput (ops/sec) | p50 (ms) | p95 (ms) | p99 (ms) | Min (ms) | Max (ms) | Errors |
|-----------|-------------|----------------------|----------|----------|----------|----------|----------|--------|
| PUT | 10 | 305.4 | 18.38 | 31.61 | 120.28 | 7.80 | 2058.49 | 14 |
| PUT | 50 | 228.4 | 93.98 | 384.34 | 2216.15 | 27.05 | 2594.31 | 6259 |
| PUT | 100 | 209.4 | 187.04 | 2174.32 | 2614.26 | 14.33 | 3525.85 | 5543 |
| GET | 50 | 32899.3 | 1.61 | 3.61 | 5.29 | 0.06 | 300.13 | 0 |

## Leader Failover

Leader was killed at the 15-second mark of a 30s PUT benchmark at 50 threads.

| Operation | Concurrency | Throughput (ops/sec) | p50 (ms) | p95 (ms) | p99 (ms) | Min (ms) | Max (ms) | Errors |
|-----------|-------------|----------------------|----------|----------|----------|----------|----------|--------|
| PUT* | 50 | 206.6 | 92.74 | 757.93 | 2281.96 | 12.88 | 4912.88 | 4072 |

*\* = leader killed mid-run*

**Steady-state p99:** 2216.15 ms
**Failover p99:** 2281.96 ms

---

> **Disclaimer:** Tested on Intel(R) Core(TM) 7 150U, 16 GB RAM with all 3 nodes running on localhost.
> Numbers measure protocol overhead (Raft consensus + LSM storage), not network
> latency. Real-world performance across a network will be dominated by round-trip
> time rather than these protocol costs.
