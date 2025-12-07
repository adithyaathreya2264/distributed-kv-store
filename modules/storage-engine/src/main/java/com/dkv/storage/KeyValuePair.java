package com.dkv.storage;

public class KeyValuePair implements Comparable<KeyValuePair> {
    private final String key;
    private final String value;
    private final long timestamp;

    public KeyValuePair(String key, String value, long timestamp) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public int compareTo(KeyValuePair o) {
        return this.key.compareTo(o.key);
    }

    @Override
    public String toString() {
        return "KV{key='" + key + "', val='" + value + "', ts=" + timestamp + "}";
    }
}
