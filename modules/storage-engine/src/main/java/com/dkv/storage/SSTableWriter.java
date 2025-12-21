package com.dkv.storage;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Writes MemTable to disk as SSTable.
 * File format:
 * [Data Block]
 * Entry: [KeyLen][Key][ValLen][Val]
 * [Index Block]
 * Entry: [KeyLen][Key][Offset]
 * [Footer]
 * [IndexBlockOffset]
 */
public class SSTableWriter {
    public static void write(MemTable memTable, String filename) throws IOException {
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(filename))) {
            long currentOffset = 0;

            // Sparse index: Map Key -> Offset
            TreeMap<String, Long> index = new TreeMap<>();

            Iterator<KeyValuePair> it = memTable.iterator();
            while (it.hasNext()) {
                KeyValuePair kv = it.next();
                String key = kv.getKey();
                String value = kv.getValue();

                // Add to index every ~1KB or simply every entry for now?
                // For simplicity, let's index every entry or every N entries.
                // Indexing every entry makes it a dense index, easier for simple
                // implementation.
                index.put(key, currentOffset);

                byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                byte[] valBytes = (value == null) ? null : value.getBytes(StandardCharsets.UTF_8);

                dos.writeInt(keyBytes.length);
                dos.write(keyBytes);

                if (value == null) {
                    dos.writeInt(-1); // Tombstone
                    currentOffset += (Integer.BYTES + keyBytes.length + Integer.BYTES);
                } else {
                    dos.writeInt(valBytes.length);
                    dos.write(valBytes);
                    currentOffset += (Integer.BYTES + keyBytes.length + Integer.BYTES + valBytes.length);
                }
            }

            long indexStartOffset = currentOffset;

            // Write Index Block
            for (Map.Entry<String, Long> entry : index.entrySet()) {
                byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
                dos.writeInt(keyBytes.length);
                dos.write(keyBytes);
                dos.writeLong(entry.getValue());
            }

            // Footer
            dos.writeLong(indexStartOffset);
        }
    }
}
