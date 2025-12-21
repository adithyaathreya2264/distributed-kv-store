package com.dkv.storage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LSMStorageEngine {
    private final String dataDir;
    private MemTable memTable;
    private WriteAheadLog wal;
    private final List<SSTableReader> ssTables;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    // Rough flush threshold (e.g., 1MB for testing)
    private static final long FLUSH_THRESHOLD = 1024 * 1024;

    public LSMStorageEngine(String dataDir) throws IOException {
        this.dataDir = dataDir;
        this.memTable = new MemTable();
        // Should actually generate unique WAL filenames or manage rotation
        this.wal = new WriteAheadLog(dataDir + "/current.wal");
        this.ssTables = new ArrayList<>();

        File dir = new File(dataDir);
        File[] files = dir.listFiles((d, name) -> name.endsWith(".sst"));
        if (files != null) {
            for (File f : files) {
                ssTables.add(new SSTableReader(f.getAbsolutePath()));
            }
        }
    }

    public void put(String key, String value) throws IOException {
        lock.writeLock().lock();
        try {
            wal.append(key, value);
            memTable.put(key, value);

            if (memTable.sizeInBytes() >= FLUSH_THRESHOLD) {
                flush();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public String get(String key) throws IOException {
        lock.readLock().lock();
        try {
            // 1. Check MemTable
            KeyValuePair kv = memTable.get(key);
            if (kv != null) {
                return kv.getValue();
            }

            // 2. Check SSTables (Newest to Oldest)
            // Ideally ssTables list should be sorted by time (newest first)
            // Currently they are just added. We loaded them from disk (unsorted order from
            // listFiles?)

            for (int i = ssTables.size() - 1; i >= 0; i--) {
                String val = ssTables.get(i).get(key);
                if (val != null)
                    return val;
            }

            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    private void flush() throws IOException {
        String sstFilename = dataDir + "/sst-" + System.currentTimeMillis() + ".sst";
        SSTableWriter.write(memTable, sstFilename);

        SSTableReader reader = new SSTableReader(sstFilename);
        ssTables.add(reader);

        memTable.clear();
        wal.close();
        wal = new WriteAheadLog(dataDir + "/current.wal");
    }

    public List<SSTableReader> getSSTablesSnapshot() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(ssTables);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            wal.close();
            for (SSTableReader reader : ssTables) {
                reader.close();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}
