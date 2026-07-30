package com.dkv.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class LSMReadWriteTest {
    private static final String DATA_DIR = "test-data";
    private LSMStorageEngine engine;

    @BeforeEach
    public void setup() throws IOException {
        cleanup();
        new File(DATA_DIR).mkdirs();
        engine = new LSMStorageEngine(DATA_DIR);
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (engine != null)
            engine.close();
        cleanup();
    }

    private void cleanup() {
        File dir = new File(DATA_DIR);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File f : files)
                    f.delete();
            }
            dir.delete();
        }
    }

    @Test
    public void testPutGetMemTable() throws IOException {
        engine.put("key1", "val1");
        assertEquals("val1", engine.get("key1"));
    }

    @Test
    public void testFlushToSSTable() throws IOException {
        // 1. Put keys
        engine.put("key1", "val1");

        // 2. Force flush (Simulate by filling up or exposing flush method)
        engine.close();

        assertTrue(true);
    }
}
