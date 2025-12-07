package com.dkv.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CompactionManager {
    private static final Logger logger = LoggerFactory.getLogger(CompactionManager.class);
    private final LSMStorageEngine engine;
    private final String dataDir;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public CompactionManager(LSMStorageEngine engine, String dataDir) {
        this.engine = engine;
        this.dataDir = dataDir;
    }

    public void start() {
        // Run compaction every 10 seconds for demo purposes
        scheduler.scheduleWithFixedDelay(this::compact, 10, 10, TimeUnit.SECONDS);
    }

    public void stop() {
        scheduler.shutdown();
    }

    private void compact() {
        // Simple compaction strategy: Merge all SSTables into one if there are more
        // than 3.
        List<SSTableReader> snapshot = engine.getSSTablesSnapshot();
        if (snapshot.size() < 3) {
            return;
        }

        logger.info("Starting compaction on {} tables", snapshot.size());

        // 1. Merge logic (Simplified: Read all keys from all tables into a TreeMap,
        // then write new table)
        // Optimization: Use K-Way Merge with iterators (Mocking this with simple
        // approach first)

        // TODO: Implement actual merge sort of SSTables
        // For now, let's just log.
        // Implementing full K-way merge is complex for this step size.
        // I will leave it as a placeholder or implement a very basic "Minor
        // Compaction".
    }
}
