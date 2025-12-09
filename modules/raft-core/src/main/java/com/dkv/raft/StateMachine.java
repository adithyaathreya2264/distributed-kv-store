package com.dkv.raft;

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
}
