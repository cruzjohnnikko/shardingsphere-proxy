package org.apache.shardingsphere.example.proxy.cdc.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service for managing database migration operations
 * Handles Source DB (existing production) → Target DB (migration destination)
 */
@Slf4j
@Service
public class MigrationService {

    @Value("${migration.source.url:jdbc:postgresql://localhost:5435/source_db}")
    private String sourceDbUrl;

    @Value("${migration.source.username:postgres}")
    private String sourceDbUsername;

    @Value("${migration.source.password:postgres}")
    private String sourceDbPassword;

    @Value("${migration.target.url:jdbc:postgresql://localhost:5434/target_db}")
    private String targetDbUrl;

    @Value("${migration.target.username:postgres}")
    private String targetDbUsername;

    @Value("${migration.target.password:postgres}")
    private String targetDbPassword;

    /**
     * Setup source database with sample production data
     */
    public Map<String, Object> setupSourceDatabase() {
        log.info("Setting up source database with sample data");
        
        try (Connection conn = DriverManager.getConnection(sourceDbUrl, sourceDbUsername, sourceDbPassword)) {
            // Create table
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS t_order CASCADE");
                stmt.execute("CREATE TABLE t_order (" +
                        "order_id SERIAL PRIMARY KEY, " +
                        "user_id INTEGER NOT NULL, " +
                        "status VARCHAR(50) NOT NULL, " +
                        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");
                
                // Insert sample data
                stmt.execute("INSERT INTO t_order (user_id, status) VALUES " +
                        "(1, 'completed'), (2, 'pending'), (3, 'shipped'), " +
                        "(1, 'processing'), (4, 'completed'), (2, 'cancelled'), " +
                        "(5, 'shipped'), (3, 'completed'), (6, 'pending'), (1, 'processing')");
                
                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM t_order");
                rs.next();
                int count = rs.getInt(1);
                
                log.info("Source database setup completed with {} records", count);
                return Map.of(
                    "success", true,
                    "message", "Source database initialized",
                    "recordCount", count
                );
            }
        } catch (SQLException e) {
            log.error("Error setting up source database", e);
            return Map.of(
                "success", false,
                "message", "Error: " + e.getMessage()
            );
        }
    }

    /**
     * Prepare target database (create table structure)
     */
    public Map<String, Object> prepareTargetDatabase() {
        log.info("Preparing target database");
        
        try (Connection conn = DriverManager.getConnection(targetDbUrl, targetDbUsername, targetDbPassword)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS t_order CASCADE");
                stmt.execute("CREATE TABLE t_order (" +
                        "order_id SERIAL PRIMARY KEY, " +
                        "user_id INTEGER NOT NULL, " +
                        "status VARCHAR(50) NOT NULL, " +
                        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");
                
                log.info("Target database prepared (empty table created)");
                return Map.of(
                    "success", true,
                    "message", "Target database prepared"
                );
            }
        } catch (SQLException e) {
            log.error("Error preparing target database", e);
            return Map.of(
                "success", false,
                "message", "Error: " + e.getMessage()
            );
        }
    }

    /**
     * Copy initial data from source to target (for migration baseline)
     */
    public Map<String, Object> copyInitialData() {
        log.info("Copying initial data from source to target");
        
        try {
            // Read all data from source
            List<Map<String, Object>> sourceData = getSourceRecords();
            
            if (sourceData.isEmpty()) {
                log.info("No data to copy from source");
                return Map.of(
                    "success", true,
                    "recordsCopied", 0,
                    "message", "No data in source to copy"
                );
            }
            
            // Insert all data into target
            try (Connection conn = DriverManager.getConnection(targetDbUrl, targetDbUsername, targetDbPassword)) {
                String insertSql = "INSERT INTO t_order (order_id, user_id, status) VALUES (?, ?, ?) " +
                        "ON CONFLICT (order_id) DO UPDATE SET user_id = EXCLUDED.user_id, status = EXCLUDED.status";
                
                try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                    for (Map<String, Object> row : sourceData) {
                        pstmt.setInt(1, (Integer) row.get("order_id"));
                        pstmt.setInt(2, (Integer) row.get("user_id"));
                        pstmt.setString(3, (String) row.get("status"));
                        pstmt.addBatch();
                    }
                    pstmt.executeBatch();
                }
            }
            
            log.info("Copied {} records from source to target", sourceData.size());
            return Map.of(
                "success", true,
                "recordsCopied", sourceData.size(),
                "message", String.format("Copied %d records from source to target", sourceData.size())
            );
            
        } catch (SQLException e) {
            log.error("Error copying initial data", e);
            return Map.of(
                "success", false,
                "recordsCopied", 0,
                "message", "Error: " + e.getMessage()
            );
        }
    }

    /**
     * Get record count from source database
     */
    public long getSourceRecordCount() {
        try (Connection conn = DriverManager.getConnection(sourceDbUrl, sourceDbUsername, sourceDbPassword);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM t_order")) {
            
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            log.error("Error getting source record count", e);
        }
        return 0;
    }

    /**
     * Get record count from target database
     */
    public long getTargetRecordCount() {
        try (Connection conn = DriverManager.getConnection(targetDbUrl, targetDbUsername, targetDbPassword);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM t_order")) {
            
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            log.error("Error getting target record count", e);
        }
        return 0;
    }

    /**
     * Get all records from source database
     */
    public List<Map<String, Object>> getSourceRecords() {
        List<Map<String, Object>> records = new ArrayList<>();
        
        try (Connection conn = DriverManager.getConnection(sourceDbUrl, sourceDbUsername, sourceDbPassword);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT order_id, user_id, status FROM t_order ORDER BY order_id LIMIT 100")) {
            
            while (rs.next()) {
                Map<String, Object> record = new HashMap<>();
                record.put("order_id", rs.getInt("order_id"));
                record.put("user_id", rs.getInt("user_id"));
                record.put("status", rs.getString("status"));
                records.add(record);
            }
        } catch (SQLException e) {
            log.error("Error getting source records", e);
        }
        
        return records;
    }

    /**
     * Get all records from target database
     */
    public List<Map<String, Object>> getTargetRecords() {
        List<Map<String, Object>> records = new ArrayList<>();
        
        try (Connection conn = DriverManager.getConnection(targetDbUrl, targetDbUsername, targetDbPassword);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT order_id, user_id, status FROM t_order ORDER BY order_id LIMIT 100")) {
            
            while (rs.next()) {
                Map<String, Object> record = new HashMap<>();
                record.put("order_id", rs.getInt("order_id"));
                record.put("user_id", rs.getInt("user_id"));
                record.put("status", rs.getString("status"));
                records.add(record);
            }
        } catch (SQLException e) {
            log.error("Error getting target records", e);
        }
        
        return records;
    }

    /**
     * Get migration status
     */
    public Map<String, Object> getMigrationStatus() {
        long sourceCount = getSourceRecordCount();
        long targetCount = getTargetRecordCount();
        long lag = sourceCount - targetCount;
        double progress = sourceCount > 0 ? (targetCount * 100.0 / sourceCount) : 0;
        
        String status;
        if (targetCount == 0) {
            status = "Not started";
        } else if (lag == 0) {
            status = "Synchronized";
        } else if (lag < 5) {
            status = "Almost synchronized (lag: " + lag + " records)";
        } else {
            status = "Replicating (lag: " + lag + " records)";
        }
        
        return Map.of(
            "sourceCount", sourceCount,
            "targetCount", targetCount,
            "lag", lag,
            "progress", Math.round(progress),
            "status", status,
            "synchronized", lag == 0
        );
    }

    /**
     * Verify sync between source and target
     */
    public Map<String, Object> verifySync() {
        long sourceCount = getSourceRecordCount();
        long targetCount = getTargetRecordCount();
        boolean inSync = (sourceCount == targetCount);
        
        return Map.of(
            "inSync", inSync,
            "sourceCount", sourceCount,
            "targetCount", targetCount,
            "message", inSync ? "Databases are synchronized! ✓" : "Databases not in sync. Lag: " + (sourceCount - targetCount)
        );
    }

    /**
     * Clear source database
     */
    public void clearSourceDatabase() {
        log.info("Clearing migration source database");
        
        try (Connection conn = DriverManager.getConnection(sourceDbUrl, sourceDbUsername, sourceDbPassword);
             Statement stmt = conn.createStatement()) {
            // Use DELETE instead of TRUNCATE to avoid breaking CDC (ShardingSphere doesn't support TRUNCATE in WAL)
            stmt.execute("DELETE FROM t_order");
            log.info("Migration source database cleared");
        } catch (SQLException e) {
            log.error("Error clearing migration source database", e);
            throw new RuntimeException("Failed to clear migration source database: " + e.getMessage());
        }
    }
}

