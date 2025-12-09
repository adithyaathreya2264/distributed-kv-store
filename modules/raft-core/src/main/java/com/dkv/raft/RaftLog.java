package com.dkv.raft;

import com.dkv.raft.proto.RaftProtos.LogEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manages the Raft log entries.
 * Currently in-memory. Will interface with WAL/StorageEngine later.
 */
public class RaftLog {
    private final List<LogEntry> entries;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public RaftLog() {
        this.entries = new ArrayList<>();
    }

    public long getLastLogIndex() {
        lock.readLock().lock();
        try {
            if (entries.isEmpty()) {
                return 0;
            }
            return entries.get(entries.size() - 1).getIndex();
        } finally {
            lock.readLock().unlock();
        }
    }

    public long getLastLogTerm() {
        lock.readLock().lock();
        try {
            if (entries.isEmpty()) {
                return 0;
            }
            return entries.get(entries.size() - 1).getTerm();
        } finally {
            lock.readLock().unlock();
        }
    }

    public void append(LogEntry entry) {
        lock.writeLock().lock();
        try {
            entries.add(entry);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public LogEntry getEntry(long index) {
        lock.readLock().lock();
        try {
            if (index <= 0 || index > entries.size()) {
                return null;
            }
            // index is 1-based, list is 0-based
            return entries.get((int) (index - 1));
        } finally {
            lock.readLock().unlock();
        }
    }

    public void truncateFrom(long index) {
        lock.writeLock().lock();
        try {
            // If index is 1, clear everything.
            // If index is size+1, do nothing.
            if (index <= 0)
                return;

            int listIndex = (int) (index - 1);
            if (listIndex < entries.size()) {
                entries.subList(listIndex, entries.size()).clear();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}
