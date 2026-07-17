package com.dkv.raft;

import com.dkv.raft.proto.RaftProtos.LogEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class RaftLogTest {
    private RaftLog log;

    @BeforeEach
    public void setup() {
        log = new RaftLog();
    }

    @Test
    public void testAppendAndRetrieve() {
        LogEntry entry1 = LogEntry.newBuilder().setTerm(1).setIndex(1).build();
        LogEntry entry2 = LogEntry.newBuilder().setTerm(1).setIndex(2).build();

        log.append(entry1);
        log.append(entry2);

        assertEquals(2, log.getLastLogIndex());
        assertEquals(1, log.getLastLogTerm());
        assertEquals(entry1, log.getEntry(1));
        assertEquals(entry2, log.getEntry(2));
    }

    @Test
    public void testTruncate() {
        log.append(LogEntry.newBuilder().setTerm(1).setIndex(1).build());
        log.append(LogEntry.newBuilder().setTerm(1).setIndex(2).build());
        log.append(LogEntry.newBuilder().setTerm(2).setIndex(3).build());

        log.truncateFrom(2);

        assertEquals(1, log.getLastLogIndex());
        assertNull(log.getEntry(2));
        assertNotNull(log.getEntry(1));
    }
}
