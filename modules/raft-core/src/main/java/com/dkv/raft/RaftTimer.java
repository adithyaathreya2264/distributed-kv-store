package com.dkv.raft;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class RaftTimer {
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> currentTask;
    private final Runnable action;
    private final int minTimeoutMs;
    private final int maxTimeoutMs;
    private final Random random = new Random();

    public RaftTimer(Runnable action, int minTimeoutMs, int maxTimeoutMs) {
        this.action = action;
        this.minTimeoutMs = minTimeoutMs;
        this.maxTimeoutMs = maxTimeoutMs;
    }

    public synchronized void reset() {
        if (currentTask != null && !currentTask.isDone()) {
            currentTask.cancel(false);
        }
        int timeout = minTimeoutMs + random.nextInt(maxTimeoutMs - minTimeoutMs);
        currentTask = scheduler.schedule(action, timeout, TimeUnit.MILLISECONDS);
    }

    public synchronized void stop() {
        if (currentTask != null) {
            currentTask.cancel(false);
        }
    }
}
