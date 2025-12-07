package com.dkv.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class WALTest {
    private static final String WAL_FILE = "test.wal";
    private WriteAheadLog wal;

    @BeforeEach
    public void setup() throws IOException {
        cleanup();
        wal = new WriteAheadLog(WAL_FILE);
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (wal != null)
            wal.close();
        cleanup();
    }

    private void cleanup() {
        File f = new File(WAL_FILE);
        if (f.exists())
            f.delete();
    }

    @Test
    public void testWriteAndRecover() throws IOException {
        wal.append("key1", "value1");
        wal.append("key2", "value2");
        wal.append("key3", null); // deletion

        wal.close();

        // Recover
        List<KeyValuePair> recovered = WriteAheadLog.recover(WAL_FILE);
        assertEquals(3, recovered.size());

        assertEquals("key1", recovered.get(0).getKey());
        assertEquals("value1", recovered.get(0).getValue());

        assertEquals("key2", recovered.get(1).getKey());
        assertEquals("value2", recovered.get(1).getValue());

        assertEquals("key3", recovered.get(2).getKey());
        assertNull(recovered.get(2).getValue());
    }
}
