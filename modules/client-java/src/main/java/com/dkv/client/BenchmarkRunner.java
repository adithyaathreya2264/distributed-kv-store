package com.dkv.client;

import java.util.*;
import java.util.concurrent.*;

/**
 * Load-generating benchmark for the DKV cluster.
 *
 * Usage:
 *   BenchmarkRunner <seeds> <operation> <threads> <durationSec> <valueSizeBytes>
 *
 * Example:
 *   BenchmarkRunner localhost:8081,localhost:8082,localhost:8083 PUT 50 30 100
 *
 * Each thread creates its own DKVClient instance (and therefore its own Netty
 * connection pool) to avoid single-channel contention.  Latencies are recorded
 * as raw nanosecond deltas in a pre-sized long[] to minimise GC pressure.
 *
 * At the end of the run the merged latency array is sorted once and percentile
 * values are picked by index — no external library needed.
 */
public class BenchmarkRunner {

    // ── per-thread result container ──
    private static final class ThreadResult {
        final long[] latenciesNs;
        final int count;
        final long errors;

        ThreadResult(long[] latenciesNs, int count, long errors) {
            this.latenciesNs = latenciesNs;
            this.count = count;
            this.errors = errors;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: BenchmarkRunner <seeds> <PUT|GET> <threads> <durationSec> <valueSizeBytes>");
            System.err.println("  seeds: comma-separated host:port list, e.g. localhost:8081,localhost:8082,localhost:8083");
            System.exit(1);
        }

        List<String> seeds = Arrays.asList(args[0].split(","));
        String operation = args[1].toUpperCase();
        int threadCount = Integer.parseInt(args[2]);
        int durationSec = Integer.parseInt(args[3]);
        int valueSize = Integer.parseInt(args[4]);

        if (!operation.equals("PUT") && !operation.equals("GET")) {
            System.err.println("Operation must be PUT or GET");
            System.exit(1);
        }

        System.out.printf("=== DKV Benchmark ===%n");
        System.out.printf("  Seeds:      %s%n", seeds);
        System.out.printf("  Operation:  %s%n", operation);
        System.out.printf("  Threads:    %d%n", threadCount);
        System.out.printf("  Duration:   %d s%n", durationSec);
        System.out.printf("  Value size: %d bytes%n%n", valueSize);

        // For GET benchmarks, pre-populate keys so there is data to read
        if (operation.equals("GET")) {
            System.out.println("Pre-populating 1,000 keys for GET benchmark...");
            prepopulateKeys(seeds, 1_000, valueSize);
            System.out.println("Pre-population complete.\n");
        }

        // Build a fixed-length random value (reused across all threads)
        String fixedValue = randomValue(valueSize);

        // Latch so all threads start at the same moment
        CountDownLatch startLatch = new CountDownLatch(1);
        long durationNs = TimeUnit.SECONDS.toNanos(durationSec);

        // Submit worker threads
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        List<Future<ThreadResult>> futures = new ArrayList<>();

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            futures.add(pool.submit(() -> runWorker(
                    seeds, operation, threadId, fixedValue,
                    startLatch, durationNs)));
        }

        // Fire!
        System.out.printf("Starting %d threads for %d seconds...%n", threadCount, durationSec);
        long wallStart = System.nanoTime();
        startLatch.countDown();

        // Collect results
        List<ThreadResult> results = new ArrayList<>();
        for (Future<ThreadResult> f : futures) {
            results.add(f.get());
        }
        long wallElapsed = System.nanoTime() - wallStart;
        pool.shutdown();

        // Close is not needed per-thread since each thread closes its own client,
        // but shut down the pool.
        pool.awaitTermination(5, TimeUnit.SECONDS);

        // Merge all latencies
        int totalOps = 0;
        long totalErrors = 0;
        for (ThreadResult r : results) {
            totalOps += r.count;
            totalErrors += r.errors;
        }

        long[] allLatencies = new long[totalOps];
        int pos = 0;
        for (ThreadResult r : results) {
            System.arraycopy(r.latenciesNs, 0, allLatencies, pos, r.count);
            pos += r.count;
        }

        // Sort for percentile computation
        Arrays.sort(allLatencies);

        double wallSec = wallElapsed / 1_000_000_000.0;
        double throughput = totalOps / wallSec;

        // Percentiles (index-based on sorted array)
        long p50 = percentile(allLatencies, 50);
        long p95 = percentile(allLatencies, 95);
        long p99 = percentile(allLatencies, 99);
        long min = allLatencies.length > 0 ? allLatencies[0] : 0;
        long max = allLatencies.length > 0 ? allLatencies[allLatencies.length - 1] : 0;

        // Human-readable output
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════╗");
        System.out.println("║               BENCHMARK RESULTS                      ║");
        System.out.println("╠══════════════════════════════════════════════════════╣");
        System.out.printf( "║  Operation:      %-36s ║%n", operation);
        System.out.printf( "║  Threads:        %-36d ║%n", threadCount);
        System.out.printf( "║  Duration:       %-36s ║%n", String.format("%.2f s", wallSec));
        System.out.printf( "║  Total ops:      %-36d ║%n", totalOps);
        System.out.printf( "║  Errors:         %-36d ║%n", totalErrors);
        System.out.printf( "║  Throughput:     %-36s ║%n", String.format("%.1f ops/sec", throughput));
        System.out.printf( "║  p50 latency:    %-36s ║%n", formatNanos(p50));
        System.out.printf( "║  p95 latency:    %-36s ║%n", formatNanos(p95));
        System.out.printf( "║  p99 latency:    %-36s ║%n", formatNanos(p99));
        System.out.printf( "║  Min latency:    %-36s ║%n", formatNanos(min));
        System.out.printf( "║  Max latency:    %-36s ║%n", formatNanos(max));
        System.out.println("╚══════════════════════════════════════════════════════╝");

        // Machine-parseable CSV line (for benchmark.ps1 to capture)
        // Format: BENCHMARK_CSV,<op>,<threads>,<totalOps>,<throughput>,<p50Ms>,<p95Ms>,<p99Ms>,<minMs>,<maxMs>,<errors>
        System.out.printf("BENCHMARK_CSV,%s,%d,%d,%.1f,%.2f,%.2f,%.2f,%.2f,%.2f,%d%n",
                operation, threadCount, totalOps, throughput,
                nsToMs(p50), nsToMs(p95), nsToMs(p99),
                nsToMs(min), nsToMs(max), totalErrors);
    }

    /**
     * Worker loop for a single benchmark thread.
     */
    private static ThreadResult runWorker(
            List<String> seeds, String operation, int threadId,
            String fixedValue, CountDownLatch startLatch, long durationNs) {

        // Each thread gets its own client → its own Netty connections
        DKVClient client = new DKVClient(seeds);

        // Pre-allocate latency buffer (generous upper bound: ~100k ops/thread)
        int capacity = 500_000;
        long[] latencies = new long[capacity];
        int count = 0;
        long errors = 0;

        try {
            startLatch.await();
            long deadline = System.nanoTime() + durationNs;

            while (System.nanoTime() < deadline) {
                String key;
                long start = System.nanoTime();
                try {
                    if (operation.equals("PUT")) {
                        key = "bench-" + threadId + "-" + count;
                        client.put(key, fixedValue).get(5, TimeUnit.SECONDS);
                    } else {
                        // GET: read from pre-populated key space
                        int keyIdx = ThreadLocalRandom.current().nextInt(1_000);
                        key = "preload-" + keyIdx;
                        client.get(key).get(5, TimeUnit.SECONDS);
                    }
                    long elapsed = System.nanoTime() - start;

                    // Record latency (grow array if needed)
                    if (count >= latencies.length) {
                        latencies = Arrays.copyOf(latencies, latencies.length * 2);
                    }
                    latencies[count] = elapsed;
                    count++;
                } catch (Exception e) {
                    errors++;
                    // Don't record latency for failed requests
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            client.close();
        }

        return new ThreadResult(latencies, count, errors);
    }

    /**
     * Pre-populates keys so GET benchmarks have data to read.
     */
    private static void prepopulateKeys(List<String> seeds, int keyCount, int valueSize) {
        DKVClient client = new DKVClient(seeds);
        String value = randomValue(valueSize);

        try {
            for (int i = 0; i < keyCount; i++) {
                try {
                    client.put("preload-" + i, value).get(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                    System.err.printf("  Warning: failed to preload key %d: %s%n", i, e.getMessage());
                }

                if ((i + 1) % 1000 == 0) {
                    System.out.printf("  Pre-populated %d / %d keys%n", i + 1, keyCount);
                }
            }
        } finally {
            client.close();
        }
    }

    // ── helpers ──

    private static long percentile(long[] sorted, int p) {
        if (sorted.length == 0) return 0;
        int idx = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(idx, sorted.length - 1))];
    }

    private static double nsToMs(long ns) {
        return ns / 1_000_000.0;
    }

    private static String formatNanos(long ns) {
        if (ns < 1_000) return ns + " ns";
        if (ns < 1_000_000) return String.format("%.1f µs", ns / 1_000.0);
        if (ns < 1_000_000_000) return String.format("%.2f ms", ns / 1_000_000.0);
        return String.format("%.2f s", ns / 1_000_000_000.0);
    }

    private static String randomValue(int size) {
        StringBuilder sb = new StringBuilder(size);
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        for (int i = 0; i < size; i++) {
            sb.append((char) ('a' + rng.nextInt(26)));
        }
        return sb.toString();
    }
}
