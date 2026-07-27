package com.dkv.storage;

import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class MemTable {
    // ConcurrentSkipListMap is thread-safe and sorted
    private final ConcurrentSkipListMap<String, KeyValuePair> table;
    private final AtomicLong sizeInBytes;

    public MemTable() {
        this.table = new ConcurrentSkipListMap<>();
        this.sizeInBytes = new AtomicLong(0);
    }

    public void put(String key, String value) {
        long timestamp = System.currentTimeMillis();
        KeyValuePair kv = new KeyValuePair(key, value, timestamp);
        table.put(key, kv);
        // Rough estimation of size: strings + overhead
        sizeInBytes.addAndGet(key.length() + value.length() + Long.BYTES);
    }

    public KeyValuePair get(String key) {
        return table.get(key);
    }

    public void delete(String key) {
        // Tombstone mechanism: value = null or specific marker
        long timestamp = System.currentTimeMillis();
        KeyValuePair kv = new KeyValuePair(key, null, timestamp);
        table.put(key, kv);
    }

    public long size() {
        return table.size();
    }

    public long sizeInBytes() {
        return sizeInBytes.get();
    }

    public void clear() {
        table.clear();
        sizeInBytes.set(0);
    }

    public Iterator<KeyValuePair> iterator() {
        return table.values().iterator();
    }
}
