package com.dkv.raft;

import java.util.Map;

/**
 * Interface for the state machine that applies committed log entries.
 */
public interface StateMachine {
    /**
     * Apply a committed command to the state machine.
     * 
     * @param command The binary command from the log entry.
     */
    void apply(byte[] command);

    /**
     * Returns a snapshot of all current state as key-value pairs.
     * Used during log compaction.
     */
    Map<String, String> getSnapshotData();

    /**
     * Replaces the entire state machine state from a snapshot.
     * Used when receiving an InstallSnapshot RPC.
     */
    void restoreFromSnapshot(Map<String, String> snapshotData);
}
