package org.apache.shardingsphere.example.proxy.cdc.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Replication Service for Read-Write Splitting
 * Handles manual replication between write and read databases
 */
@Slf4j
@Service
public class ReplicationService {

    @Value("${write.datasource.url}")
    private String writeDbUrl;

    @Value("${write.datasource.username}")
    private String writeDbUsername;

    @Value("${write.datasource.password}")
    private String writeDbPassword;

    @Value("${read.datasource.url}")
    private String readDbUrl;

    @Value("${read.datasource.username}")
    private String readDbUsername;

    @Value("${read.datasource.password}")
    private String readDbPassword;

    private final AtomicBoolean replicationEnabled = new AtomicBoolean(false);
    private ScheduledExecutorService replicationExecutor;

    public void enableReplication() {
        if (replicationEnabled.get()) {
            log.warn("Replication is already enabled");
            return;
        }

        replicationEnabled.set(true);
        log.info("Enabling replication from write_db to read_db");

        // Perform initial full sync
        syncWriteToRead();

        // Schedule periodic sync every 5 seconds
        replicationExecutor = Executors.newSingleThreadScheduledExecutor();
        replicationExecutor.scheduleAtFixedRate(this::syncWriteToRead, 5, 5, TimeUnit.SECONDS);

        log.info("Replication enabled successfully");
    }

    public void disableReplication() {
        if (!replicationEnabled.get()) {
            log.warn("Replication is already disabled");
            return;
        }

        replicationEnabled.set(false);
        if (replicationExecutor != null) {
            replicationExecutor.shutdownNow();
            replicationExecutor = null;
        }

        log.info("Replication disabled");
    }

    public boolean isReplicationEnabled() {
        return replicationEnabled.get();
    }

    public void syncWriteToRead() {
        // Only auto-sync if replication is enabled
        if (!replicationEnabled.get()) {
            return;
        }
        performSync();
    }
    
    /**
     * Performs the actual sync operation (can be called manually even when replication is disabled)
     */
    private void performSync() {
        try {
            log.debug("Syncing data from write_db to read_db");

            // Read all data from write DB
            List<Map<String, Object>> writeData = new ArrayList<>();
            try (Connection conn = DriverManager.getConnection(writeDbUrl, writeDbUsername, writeDbPassword);
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT order_id, user_id, status FROM t_order ORDER BY order_id")) {

                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    row.put("order_id", rs.getInt("order_id"));
                    row.put("user_id", rs.getInt("user_id"));
                    row.put("status", rs.getString("status"));
                    writeData.add(row);
                }
            }

            // Clear and repopulate read DB
            try (Connection conn = DriverManager.getConnection(readDbUrl, readDbUsername, readDbPassword)) {
                // Clear existing data (Use DELETE instead of TRUNCATE to avoid breaking CDC)
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("DELETE FROM t_order");
                }

                // Insert all data from write DB
                String insertSql = "INSERT INTO t_order (order_id, user_id, status) VALUES (?, ?, ?)";
                try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                    for (Map<String, Object> row : writeData) {
                        pstmt.setInt(1, (Integer) row.get("order_id"));
                        pstmt.setInt(2, (Integer) row.get("user_id"));
                        pstmt.setString(3, (String) row.get("status"));
                        pstmt.addBatch();
                    }
                    pstmt.executeBatch();
                }
            }

            log.info("Synced {} records from write_db to read_db", writeData.size());

        } catch (SQLException e) {
            log.error("Error syncing write to read", e);
            throw new RuntimeException("Failed to sync databases", e);
        }
    }
    
    /**
     * Manually trigger sync regardless of replication status
     */
    public void manualSync() {
        log.info("Manual sync triggered");
        performSync();
    }

    public List<Map<String, Object>> getWriteData() {
        return getData(writeDbUrl, writeDbUsername, writeDbPassword);
    }

    public List<Map<String, Object>> getReadData() {
        return getData(readDbUrl, readDbUsername, readDbPassword);
    }

    public long getWriteRecordCount() {
        return getRecordCount(writeDbUrl, writeDbUsername, writeDbPassword);
    }

    public long getReadRecordCount() {
        return getRecordCount(readDbUrl, readDbUsername, readDbPassword);
    }

    private List<Map<String, Object>> getData(String url, String username, String password) {
        List<Map<String, Object>> results = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(url, username, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT order_id, user_id, status FROM t_order ORDER BY order_id")) {

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("order_id", rs.getInt("order_id"));
                row.put("user_id", rs.getInt("user_id"));
                row.put("status", rs.getString("status"));
                results.add(row);
            }
        } catch (SQLException e) {
            log.error("Error getting data from database: {}", url, e);
        }
        return results;
    }

    private long getRecordCount(String url, String username, String password) {
        try (Connection conn = DriverManager.getConnection(url, username, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM t_order")) {

            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            log.error("Error getting record count from database: {}", url, e);
        }
        return 0;
    }

    public void switchDatabases() {
        log.info("Switching read and write database assignments");

        // Swap the URLs
        String tempUrl = writeDbUrl;
        String tempUsername = writeDbUsername;
        String tempPassword = writeDbPassword;

        writeDbUrl = readDbUrl;
        writeDbUsername = readDbUsername;
        writeDbPassword = readDbPassword;

        readDbUrl = tempUrl;
        readDbUsername = tempUsername;
        readDbPassword = tempPassword;

        log.info("Databases switched: write_db <-> read_db");

        // If replication is enabled, sync immediately
        if (replicationEnabled.get()) {
            syncWriteToRead();
        }
    }
    
    /**
     * Reset the sequence for order_id in a database
     */
    private void resetSequence(String url, String username, String password) {
        try (Connection conn = DriverManager.getConnection(url, username, password);
             Statement stmt = conn.createStatement()) {
            // Get the maximum order_id and set the sequence to the next value
            stmt.execute("SELECT setval('t_order_order_id_seq', (SELECT COALESCE(MAX(order_id), 0) + 1 FROM t_order), false)");
            log.debug("Reset sequence for database: {}", url);
        } catch (SQLException e) {
            log.warn("Error resetting sequence (might not exist yet): {}", e.getMessage());
        }
    }

    /**
     * Insert a record directly into the write database
     */
    public void insertToWriteDb(int userId, String status) {
        try (Connection conn = DriverManager.getConnection(writeDbUrl, writeDbUsername, writeDbPassword)) {
            // First, ensure the sequence is in sync
            resetSequence(writeDbUrl, writeDbUsername, writeDbPassword);
            
            String insertSql = "INSERT INTO t_order (user_id, status) VALUES (?, ?)";
            try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                pstmt.setInt(1, userId);
                pstmt.setString(2, status);
                pstmt.executeUpdate();
                log.info("Inserted record to write_db: userId={}, status={}", userId, status);
            }
        } catch (SQLException e) {
            log.error("Error inserting to write database", e);
            throw new RuntimeException("Failed to insert to write database", e);
        }
    }
    
    /**
     * Search records from the read database only (demonstrates read-write splitting)
     */
    public List<Map<String, Object>> searchReadDatabase(String searchType, int searchValue) {
        List<Map<String, Object>> results = new ArrayList<>();
        String sql = "SELECT order_id, user_id, status FROM t_order WHERE " + searchType + " = ? ORDER BY order_id";
        
        log.info("Searching READ database for {}={}", searchType, searchValue);
        
        try (Connection conn = DriverManager.getConnection(readDbUrl, readDbUsername, readDbPassword);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setInt(1, searchValue);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    row.put("order_id", rs.getInt("order_id"));
                    row.put("user_id", rs.getInt("user_id"));
                    row.put("status", rs.getString("status"));
                    results.add(row);
                }
            }
            
            log.info("Found {} records in READ database", results.size());
        } catch (SQLException e) {
            log.error("Error searching read database", e);
            throw new RuntimeException("Failed to search read database", e);
        }
        
        return results;
    }
}

