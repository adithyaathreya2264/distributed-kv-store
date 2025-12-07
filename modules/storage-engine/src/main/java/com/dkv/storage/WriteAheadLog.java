package com.dkv.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Write-Ahead Log.
 * Format per entry:
 * [Timestamp (long)] [Key Length (int)] [Key (bytes)] [Value Length (int)]
 * [Value (bytes)]
 * 
 * If Value Length is -1, it's a deletion (tombstone).
 */
public class WriteAheadLog {
    private static final Logger logger = LoggerFactory.getLogger(WriteAheadLog.class);
    private final FileChannel logChannel;
    private final Path path;

    public WriteAheadLog(String filename) throws IOException {
        this.path = Paths.get(filename);
        this.logChannel = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ); // Read needed for recovery? Or just separate reader?
    }

    public synchronized void append(String key, String value) throws IOException {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valBytes = (value == null) ? null : value.getBytes(StandardCharsets.UTF_8);

        int recordSize = Long.BYTES + Integer.BYTES + keyBytes.length + Integer.BYTES
                + (value == null ? 0 : valBytes.length);
        ByteBuffer buffer = ByteBuffer.allocate(recordSize);

        buffer.putLong(System.currentTimeMillis());
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);

        if (value == null) {
            buffer.putInt(-1); // Tombstone marker
        } else {
            buffer.putInt(valBytes.length);
            buffer.put(valBytes);
        }

        buffer.flip();
        while (buffer.hasRemaining()) {
            logChannel.write(buffer);
        }
        // Force sync for durability - costly but safe.
        // In real systems, batched or done periodically.
        logChannel.force(false);
    }

    public void close() throws IOException {
        logChannel.close();
    }

    // Static method for recovery
    public static List<KeyValuePair> recover(String filename) throws IOException {
        List<KeyValuePair> entries = new ArrayList<>();
        File file = new File(filename);
        if (!file.exists())
            return entries;

        try (FileInputStream fis = new FileInputStream(file);
                DataInputStream dis = new DataInputStream(new BufferedInputStream(fis))) {

            while (dis.available() > 0) {
                try {
                    long timestamp = dis.readLong();
                    int keyLen = dis.readInt();
                    byte[] keyBytes = new byte[keyLen];
                    dis.readFully(keyBytes);
                    String key = new String(keyBytes, StandardCharsets.UTF_8);

                    int valLen = dis.readInt();
                    String value = null;
                    if (valLen != -1) {
                        byte[] valBytes = new byte[valLen];
                        dis.readFully(valBytes);
                        value = new String(valBytes, StandardCharsets.UTF_8);
                    }

                    entries.add(new KeyValuePair(key, value, timestamp));
                } catch (EOFException e) {
                    break; // End of file
                }
            }
        }
        return entries;
    }
}
