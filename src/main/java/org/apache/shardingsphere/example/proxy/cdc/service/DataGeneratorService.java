package org.apache.shardingsphere.example.proxy.cdc.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Data Generator Service
 */
@Slf4j
@Service
public class DataGeneratorService {

    @Value("${source.datasource.url}")
    private String sourceDbUrl;

    @Value("${source.datasource.username}")
    private String sourceDbUsername;

    @Value("${source.datasource.password}")
    private String sourceDbPassword;

    @Value("${target.datasource.url}")
    private String targetDbUrl;

    @Value("${target.datasource.username}")
    private String targetDbUsername;

    @Value("${target.datasource.password}")
    private String targetDbPassword;

    private final Random random = new Random();
    private ScheduledExecutorService generatorExecutor;
    private volatile boolean generatorRunning = false;
    private int generatorInterval = 1000;

    @PreDestroy
    public void destroy() {
        stopGenerator();
    }

    public void startGenerator(int intervalMs, String operationType) {
        if (generatorRunning) {
            log.warn("Generator is already running");
            return;
        }

        generatorInterval = intervalMs;
        generatorRunning = true;
        generatorExecutor = Executors.newSingleThreadScheduledExecutor();

        generatorExecutor.scheduleAtFixedRate(() -> {
            try {
                if (!generatorRunning) {
                    return;
                }

                switch (operationType.toUpperCase()) {
                    case "INSERT":
                        generateData(1);
                        break;
                    case "UPDATE":
                        updateRandomData();
                        break;
                    case "DELETE":
                        deleteRandomData();
                        break;
                    case "MIXED":
                    default:
                        int operation = random.nextInt(100);
                        if (operation < 60) {
                            generateData(1); // 60% INSERT
                        } else if (operation < 90) {
                            updateRandomData(); // 30% UPDATE
                        } else {
                            deleteRandomData(); // 10% DELETE
                        }
                        break;
                }
            } catch (Exception e) {
                log.error("Error in generator", e);
            }
        }, 0, intervalMs, TimeUnit.MILLISECONDS);

        log.info("Data generator started with interval {}ms, operation: {}", intervalMs, operationType);
    }

    public void stopGenerator() {
        generatorRunning = false;
        if (generatorExecutor != null) {
            generatorExecutor.shutdownNow();
            generatorExecutor = null;
        }
        log.info("Data generator stopped");
    }

    public boolean isGeneratorRunning() {
        return generatorRunning;
    }

    public int getGeneratorInterval() {
        return generatorInterval;
    }

    private void updateRandomData() {
        try (Connection conn = DriverManager.getConnection(sourceDbUrl, sourceDbUsername, sourceDbPassword)) {
            String selectSql = "SELECT order_id FROM t_order ORDER BY RANDOM() LIMIT 1";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(selectSql)) {
                if (rs.next()) {
                    int orderId = rs.getInt("order_id");
                    String[] statuses = {"new", "processing", "completed", "cancelled"};
                    String newStatus = statuses[random.nextInt(statuses.length)];
                    updateData(orderId, newStatus);
                }
            }
        } catch (SQLException e) {
            log.error("Error updating random data", e);
        }
    }

    private void deleteRandomData() {
        try (Connection conn = DriverManager.getConnection(sourceDbUrl, sourceDbUsername, sourceDbPassword)) {
            String selectSql = "SELECT order_id FROM t_order ORDER BY RANDOM() LIMIT 1";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(selectSql)) {
                if (rs.next()) {
                    int orderId = rs.getInt("order_id");
                    deleteData(orderId);
                }
            }
        } catch (SQLException e) {
            log.error("Error deleting random data", e);
        }
    }

    public void generateData(int count) {
        try (Connection conn = DriverManager.getConnection(sourceDbUrl, sourceDbUsername, sourceDbPassword)) {
            String sql = "INSERT INTO t_order (user_id, status) VALUES (?, ?)";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (int i = 0; i < count; i++) {
                    ps.setInt(1, random.nextInt(100));
                    ps.setString(2, "new");
                    ps.executeUpdate();
                }
            }
            log.debug("Generated {} records in source database", count);
        } catch (SQLException e) {
            log.error("Error generating data", e);
            throw new RuntimeException("Failed to generate data", e);
        }
    }

    public void updateData(int orderId, String newStatus) {
        try (Connection conn = DriverManager.getConnection(sourceDbUrl, sourceDbUsername, sourceDbPassword)) {
            String sql = "UPDATE t_order SET status = ? WHERE order_id = ?";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, newStatus);
                ps.setInt(2, orderId);
                int updated = ps.executeUpdate();
                log.debug("Updated {} record(s) in source database", updated);
            }
        } catch (SQLException e) {
            log.error("Error updating data", e);
            throw new RuntimeException("Failed to update data", e);
        }
    }

    public void deleteData(int orderId) {
        try (Connection conn = DriverManager.getConnection(sourceDbUrl, sourceDbUsername, sourceDbPassword)) {
            String sql = "DELETE FROM t_order WHERE order_id = ?";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setInt(1, orderId);
                int deleted = ps.executeUpdate();
                log.debug("Deleted {} record(s) from source database", deleted);
            }
        } catch (SQLException e) {
            log.error("Error deleting data", e);
            throw new RuntimeException("Failed to delete data", e);
        }
    }

    public void clearSourceData() {
        try (Connection conn = DriverManager.getConnection(sourceDbUrl, sourceDbUsername, sourceDbPassword)) {
            String sql = "TRUNCATE TABLE t_order";
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(sql);
                log.info("Cleared all data from source database");
            }
        } catch (SQLException e) {
            log.error("Error clearing source data", e);
            throw new RuntimeException("Failed to clear source data", e);
        }
    }

    public void clearTargetData() {
        try (Connection conn = DriverManager.getConnection(targetDbUrl, targetDbUsername, targetDbPassword)) {
            String sql = "TRUNCATE TABLE t_order";
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(sql);
                log.info("Cleared all data from target database");
            }
        } catch (SQLException e) {
            log.error("Error clearing target data", e);
            throw new RuntimeException("Failed to clear target data", e);
        }
    }

    public List<Map<String, Object>> getSourceData() {
        return getData(sourceDbUrl, sourceDbUsername, sourceDbPassword);
    }

    public List<Map<String, Object>> getTargetData() {
        return getData(targetDbUrl, targetDbUsername, targetDbPassword);
    }

    public long getSourceRecordCount() {
        return getRecordCount(sourceDbUrl, sourceDbUsername, sourceDbPassword);
    }

    public long getTargetRecordCount() {
        return getRecordCount(targetDbUrl, targetDbUsername, targetDbPassword);
    }

    private List<Map<String, Object>> getData(String url, String username, String password) {
        List<Map<String, Object>> results = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(url, username, password)) {
            String sql = "SELECT order_id, user_id, status FROM t_order ORDER BY order_id";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    row.put("order_id", rs.getInt("order_id"));
                    row.put("user_id", rs.getInt("user_id"));
                    row.put("status", rs.getString("status"));
                    results.add(row);
                }
            }
        } catch (SQLException e) {
            log.error("Error fetching data from database: {}", url, e);
            throw new RuntimeException("Failed to fetch data", e);
        }
        return results;
    }

    private long getRecordCount(String url, String username, String password) {
        try (Connection conn = DriverManager.getConnection(url, username, password)) {
            String sql = "SELECT COUNT(*) FROM t_order";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        } catch (SQLException e) {
            log.error("Error getting record count from database: {}", url, e);
            throw new RuntimeException("Failed to get record count", e);
        }
        return 0;
    }
}
