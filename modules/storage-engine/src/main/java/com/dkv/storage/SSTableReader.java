package com.dkv.storage;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;

public class SSTableReader {
    private final TreeMap<String, Long> index;
    private final RandomAccessFile raf;

    public SSTableReader(String filename) throws IOException {
        this.raf = new RandomAccessFile(filename, "r");
        this.index = new TreeMap<>();
        loadIndex();
    }

    private void loadIndex() throws IOException {
        long fileLength = raf.length();
        if (fileLength < Long.BYTES)
            return;

        // Read Footer
        raf.seek(fileLength - Long.BYTES);
        long indexStartOffset = raf.readLong();

        raf.seek(indexStartOffset);
        // Read Index Block until footer
        while (raf.getFilePointer() < fileLength - Long.BYTES) {
            int keyLen = raf.readInt();
            byte[] keyBytes = new byte[keyLen];
            raf.readFully(keyBytes);
            String key = new String(keyBytes, StandardCharsets.UTF_8);
            long offset = raf.readLong();
            index.put(key, offset);
        }
    }

    public String get(String key) throws IOException {
        Long offset = index.get(key);
        if (offset == null) {
            // If dense index, key verification is direct
            // If sparse index, we'd look for floorEntry and scan
            return null;
        }

        raf.seek(offset);
        int keyLen = raf.readInt();
        // Skip key
        raf.skipBytes(keyLen);

        int valLen = raf.readInt();
        if (valLen == -1)
            return null; // Tombstone

        byte[] valBytes = new byte[valLen];
        raf.readFully(valBytes);
        return new String(valBytes, StandardCharsets.UTF_8);
    }

    public void close() throws IOException {
        raf.close();
    }
}
