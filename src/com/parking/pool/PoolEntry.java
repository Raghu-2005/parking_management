package com.parking.pool;

import java.sql.Connection;

public class PoolEntry {

    public final    Connection physicalConnection;
    public final    long       createdAt;
    public volatile long       lastUsedAt;
    public volatile long       borrowedAt;

    public PoolEntry(Connection physicalConnection) {
        this.physicalConnection = physicalConnection;
        this.createdAt          = System.currentTimeMillis();
        this.lastUsedAt         = System.currentTimeMillis();
        this.borrowedAt         = 0;
    }

    // called when connection is borrowed from pool
    public void markBorrowed() {
        this.borrowedAt  = System.currentTimeMillis();
    }

    // called when connection is returned to pool
    public void markReturned() {
        this.lastUsedAt  = System.currentTimeMillis();
        this.borrowedAt  = 0;
    }

    // check if connection is currently in use
    public boolean isBorrowed() {
        return borrowedAt > 0;
    }

    // check if connection has exceeded max lifetime
    public boolean isExpired(long maxLifetimeMs) {
        return (System.currentTimeMillis() - createdAt) > maxLifetimeMs;
    }

    // check if connection has been idle too long
    public boolean isIdle(long idleTimeoutMs) {
        return !isBorrowed() &&
               (System.currentTimeMillis() - lastUsedAt) > idleTimeoutMs;
    }

    // check if connection has been borrowed too long — possible leak
    public boolean isLeaked(long leakThresholdMs) {
        return isBorrowed() &&
               (System.currentTimeMillis() - borrowedAt) > leakThresholdMs;
    }

    @Override
    public String toString() {
        return "PoolEntry{" +
               "createdAt="   + createdAt   +
               ", lastUsed="  + lastUsedAt  +
               ", borrowed="  + borrowedAt  +
               ", inUse="     + isBorrowed() +
               "}";
    }
}
