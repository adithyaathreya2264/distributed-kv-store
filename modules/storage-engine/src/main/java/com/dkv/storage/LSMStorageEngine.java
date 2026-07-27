package com.dkv.storage;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LSMStorageEngine {
    private final String dataDir;
    private MemTable memTable;
    private WriteAheadLog wal;
    private final List<SSTableReader> ssTables;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private static final long FLUSH_THRESHOLD = 1024 * 1024;

    public LSMStorageEngine(String dataDir) throws IOException {
        this.dataDir = dataDir;
        this.memTable = new MemTable();
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

    /**
     * Returns all non-deleted key-value pairs across SSTables and MemTable.
     * SSTables are scanned oldest-to-newest, then MemTable on top, so the
     * latest value for each key wins.
     */
    public Map<String, String> getAllKeyValues() throws IOException {
        lock.readLock().lock();
        try {
            Map<String, String> result = new TreeMap<>();

            // Scan SSTables oldest to newest
            for (SSTableReader sst : ssTables) {
                for (String key : sst.getKeys()) {
                    String val = sst.get(key);
                    if (val != null) {
                        result.put(key, val);
                    } else {
                        result.remove(key); // tombstone
                    }
                }
            }

            // MemTable on top (newest)
            Iterator<KeyValuePair> it = memTable.iterator();
            while (it.hasNext()) {
                KeyValuePair kv = it.next();
                if (kv.getValue() != null) {
                    result.put(kv.getKey(), kv.getValue());
                } else {
                    result.remove(kv.getKey()); // tombstone
                }
            }

            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Replaces the entire storage engine state with the given key-value pairs.
     * Closes all current readers, deletes SSTable/WAL files, and writes a fresh
     * SSTable from the snapshot data.
     */
    public void resetFromSnapshot(Map<String, String> snapshotKvs) throws IOException {
        lock.writeLock().lock();
        try {
            // Close existing resources
            wal.close();
            for (SSTableReader reader : ssTables) {
                reader.close();
            }
            ssTables.clear();

            // Delete SSTable and WAL files in dataDir
            File dir = new File(dataDir);
            File[] files = dir.listFiles((d, name) -> name.endsWith(".sst") || name.endsWith(".wal"));
            if (files != null) {
                for (File f : files) {
                    f.delete();
                }
            }

            // Re-init empty MemTable and WAL
            memTable = new MemTable();
            wal = new WriteAheadLog(dataDir + "/current.wal");

            // Write snapshot data as a single SSTable
            if (snapshotKvs != null && !snapshotKvs.isEmpty()) {
                MemTable snapshotMem = new MemTable();
                for (Map.Entry<String, String> entry : snapshotKvs.entrySet()) {
                    snapshotMem.put(entry.getKey(), entry.getValue());
                }
                String sstFilename = dataDir + "/sst-snapshot-" + System.currentTimeMillis() + ".sst";
                SSTableWriter.write(snapshotMem, sstFilename);
                ssTables.add(new SSTableReader(sstFilename));
            }
        } finally {
            lock.writeLock().unlock();
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
