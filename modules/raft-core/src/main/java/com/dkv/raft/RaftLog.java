package com.dkv.raft;

import com.dkv.raft.proto.RaftProtos.LogEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manages the Raft log entries.
 * Currently in-memory. Will interface with WAL/StorageEngine later.
 * Supports log compaction via truncateBefore() which removes entries
 * that have been included in a snapshot.
 */
public class RaftLog {
    private final List<LogEntry> entries;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    // After compaction, entries[0] corresponds to this logical index.
    // Before any compaction, this is 1 (first real entry is at index 1).
    private long logStartIndex = 1;

    // Snapshot metadata — the last entry included in the most recent snapshot.
    private long lastIncludedIndex = 0;
    private long lastIncludedTerm = 0;

    public RaftLog() {
        this.entries = new ArrayList<>();
    }

    public long getLastLogIndex() {
        lock.readLock().lock();
        try {
            if (entries.isEmpty()) {
                return lastIncludedIndex;
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
                return lastIncludedTerm;
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
            if (index <= lastIncludedIndex || index < logStartIndex) {
                return null; // compacted away
            }
            int listIndex = (int) (index - logStartIndex);
            if (listIndex < 0 || listIndex >= entries.size()) {
                return null;
            }
            return entries.get(listIndex);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns all entries from the given 1-based index (inclusive) to the end of the log.
     * Returns an empty list if fromIndex is beyond the log or compacted.
     */
    public List<LogEntry> getEntriesFrom(long fromIndex) {
        lock.readLock().lock();
        try {
            if (entries.isEmpty() || fromIndex > getLastLogIndex()) {
                return new ArrayList<>();
            }
            // Clamp to the start of available entries
            long effectiveFrom = Math.max(fromIndex, logStartIndex);
            int listIndex = (int) (effectiveFrom - logStartIndex);
            if (listIndex < 0 || listIndex >= entries.size()) {
                return new ArrayList<>();
            }
            return new ArrayList<>(entries.subList(listIndex, entries.size()));
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Truncates the log from the given index onwards (inclusive).
     * Used when a conflicting entry is detected during AppendEntries.
     */
    public void truncateFrom(long index) {
        lock.writeLock().lock();
        try {
            if (index <= lastIncludedIndex || index < logStartIndex) {
                return;
            }
            int listIndex = (int) (index - logStartIndex);
            if (listIndex < entries.size()) {
                entries.subList(listIndex, entries.size()).clear();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Truncates the log before the given index (exclusive).
     * Called after a snapshot is taken. Entries up to and including
     * snapshotIndex are removed, and snapshot metadata is updated.
     */
    public void truncateBefore(long snapshotIndex, long snapshotTerm) {
        lock.writeLock().lock();
        try {
            if (snapshotIndex <= lastIncludedIndex) {
                return; // already compacted past this point
            }
            lastIncludedIndex = snapshotIndex;
            lastIncludedTerm = snapshotTerm;

            // Remove all entries up to and including snapshotIndex
            int entriesToRemove = (int) (snapshotIndex - logStartIndex + 1);
            if (entriesToRemove > 0 && entriesToRemove <= entries.size()) {
                entries.subList(0, entriesToRemove).clear();
            } else if (entriesToRemove > entries.size()) {
                entries.clear();
            }
            logStartIndex = snapshotIndex + 1;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Resets the log after an InstallSnapshot. All existing entries are
     * discarded because the snapshot supersedes them.
     */
    public void resetAfterSnapshot(long snapshotIndex, long snapshotTerm) {
        lock.writeLock().lock();
        try {
            entries.clear();
            lastIncludedIndex = snapshotIndex;
            lastIncludedTerm = snapshotTerm;
            logStartIndex = snapshotIndex + 1;
        } finally {
            lock.writeLock().unlock();
        }
    }

    // Getters for snapshot metadata
    public long getLastIncludedIndex() {
        lock.readLock().lock();
        try {
            return lastIncludedIndex;
        } finally {
            lock.readLock().unlock();
        }
    }

    public long getLastIncludedTerm() {
        lock.readLock().lock();
        try {
            return lastIncludedTerm;
        } finally {
            lock.readLock().unlock();
        }
    }
}
