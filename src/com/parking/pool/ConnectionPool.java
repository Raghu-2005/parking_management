package com.parking.pool;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConnectionPool {

    private final LinkedBlockingQueue<PoolEntry> availableConnections;
    private final ConcurrentHashMap<Connection, PoolEntry> inUseConnections;
    private final int maxPoolSize;
    private final long maxLifetimeMs;
    private final long idleTimeoutMs;
    private final long leakThresholdMs;
    private final String dbUrl;
    private final String dbUser;
    private final String dbPassword;
    private final ScheduledExecutorService scheduler;

    public ConnectionPool(String dbUrl,
            String dbUser,
            String dbPassword,
            int maxPoolSize) throws SQLException {

        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
        this.maxPoolSize = maxPoolSize;
        this.maxLifetimeMs = 30 * 60 * 1000L; // 30 minutes
        this.idleTimeoutMs = 25 * 60 * 1000L; // 25 minutes (was 10 — caused drain)
        this.leakThresholdMs = 5 * 60 * 1000L; // 5 minutes

        this.availableConnections = new LinkedBlockingQueue<>(maxPoolSize);
        this.inUseConnections = new ConcurrentHashMap<>();

        // Load PostgreSQL driver once for the entire pool
        try {
            Class.forName("org.postgresql.Driver");
            System.out.println("[Pool] PostgreSQL driver loaded");
        } catch (ClassNotFoundException e) {
            throw new SQLException(
                    "PostgreSQL driver not found. Check postgresql jar in WEB-INF/lib", e);
        }

        // Create all physical connections at startup
        for (int i = 0; i < maxPoolSize; i++) {
            PoolEntry entry = new PoolEntry(createPhysicalConnection());
            availableConnections.offer(entry);
        }

        System.out.println("[Pool] Initialized with " + maxPoolSize + " connections");

        // Start monitor every 5 minutes
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(
                this::monitorTask, 5, 5, TimeUnit.MINUTES);
    }

    // ── GET CONNECTION ───────────────────────────────────
    public Connection getConnection() throws SQLException {
        try {
            PoolEntry entry = availableConnections.poll(60, TimeUnit.SECONDS);

            if (entry == null) {
                throw new SQLException("[Pool] POOL_TIMEOUT: No connection available after 60 seconds. Server is busy, please try again.");
            }

            // Replace if expired or invalid
            if (entry.isExpired(maxLifetimeMs) ||
                    !isConnectionValid(entry.physicalConnection)) {

                closeQuietly(entry.physicalConnection);
                entry = new PoolEntry(createPhysicalConnection());
                System.out.println("[Pool] Replaced expired connection on borrow");
            }

            entry.markBorrowed();
            Connection proxy = createProxy(entry);
            inUseConnections.put(proxy, entry);
            return proxy;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("[Pool] Interrupted while waiting", e);
        }
    }

    // ── RETURN CONNECTION ────────────────────────────────
    private void returnConnection(Connection proxy) {
        PoolEntry entry = inUseConnections.remove(proxy);
        if (entry == null)
            return;

        entry.markReturned();

        // Replace if expired or invalid
        if (entry.isExpired(maxLifetimeMs) ||
                !isConnectionValid(entry.physicalConnection)) {

            closeQuietly(entry.physicalConnection);
            try {
                entry = new PoolEntry(createPhysicalConnection());
                System.out.println("[Pool] Replaced connection on return");
            } catch (SQLException e) {
                System.err.println("[Pool] Failed to replace on return: " + e.getMessage());
                return;
            }
        }

        availableConnections.offer(entry);
    }

    // ── CREATE PHYSICAL CONNECTION ───────────────────────
    private Connection createPhysicalConnection() throws SQLException {
        return DriverManager.getConnection(dbUrl, dbUser, dbPassword);
    }

    // ── CREATE PROXY ─────────────────────────────────────
    private Connection createProxy(PoolEntry entry) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class[] { Connection.class },
                new InvocationHandler() {
                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args)
                            throws Throwable {
                        if ("close".equals(method.getName())) {
                            returnConnection((Connection) proxy);
                            return null;
                        }
                        return method.invoke(entry.physicalConnection, args);
                    }
                });
    }

    // ── MONITOR TASK ─────────────────────────────────────
    private void monitorTask() {
        try {
            System.out.println("[Pool] Monitor — available: " +
                    availableConnections.size() +
                    " inUse: " + inUseConnections.size());

            // Check for leaked connections
            for (ConcurrentHashMap.Entry<Connection, PoolEntry> e : inUseConnections.entrySet()) {
                if (e.getValue().isLeaked(leakThresholdMs)) {
                    System.err.println("[Pool] WARNING — possible connection leak detected");
                }
            }

            // Safely collect entries that need replacement
            // (drain first, then re-add — avoids race condition that drained pool to 0)
            List<PoolEntry> toCheck = new ArrayList<>();
            availableConnections.drainTo(toCheck);

            for (PoolEntry entry : toCheck) {
                if (entry.isIdle(idleTimeoutMs) || entry.isExpired(maxLifetimeMs)) {
                    closeQuietly(entry.physicalConnection);
                    try {
                        PoolEntry fresh = new PoolEntry(createPhysicalConnection());
                        availableConnections.offer(fresh);
                        System.out.println("[Pool] Replaced idle/expired connection");
                    } catch (SQLException ex) {
                        System.err.println("[Pool] Failed to replace: " + ex.getMessage());
                        // Don't re-add the old bad entry — just log and continue
                    }
                } else {
                    // Still good — put it back
                    availableConnections.offer(entry);
                }
            }

        } catch (Exception e) {
            System.err.println("[Pool] Monitor error: " + e.getMessage());
        }
    }

    // ── VALIDATE CONNECTION ──────────────────────────────
    private boolean isConnectionValid(Connection con) {
        try {
            return con != null && !con.isClosed() && con.isValid(2);
        } catch (SQLException e) {
            return false;
        }
    }

    // ── CLOSE QUIETLY ────────────────────────────────────
    private void closeQuietly(Connection con) {
        try {
            if (con != null && !con.isClosed())
                con.close();
        } catch (SQLException e) {
            System.err.println("[Pool] Error closing: " + e.getMessage());
        }
    }

    // ── SHUTDOWN ─────────────────────────────────────────
    public void shutdown() {
        scheduler.shutdownNow();
        for (PoolEntry entry : availableConnections)
            closeQuietly(entry.physicalConnection);
        availableConnections.clear();
        for (PoolEntry entry : inUseConnections.values())
            closeQuietly(entry.physicalConnection);
        inUseConnections.clear();
        System.out.println("[Pool] Shutdown complete");
    }

    // ── STATS ────────────────────────────────────────────
    public int getAvailableCount() {
        return availableConnections.size();
    }

    public int getInUseCount() {
        return inUseConnections.size();
    }
}