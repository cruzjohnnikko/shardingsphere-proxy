package org.apache.shardingsphere.example.proxy.cdc.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.example.proxy.cdc.model.CDCEvent;
import org.apache.shardingsphere.example.proxy.cdc.model.CDCStatistics;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * CDC Client Service with Event Replication
 */
@Slf4j
@Service
public class CDCClientService {
    
    @Autowired
    private WebSocketService webSocketService;

    @Value("${cdc.server.address:localhost}")
    private String serverAddress;
    
    @Value("${cdc.server.port:33071}")
    private int serverPort;

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

    private final AtomicBoolean isStreaming = new AtomicBoolean(false);
    private final AtomicBoolean isConnected = new AtomicBoolean(false);
    private final AtomicLong totalEvents = new AtomicLong(0);
    private final AtomicLong insertCount = new AtomicLong(0);
    private final AtomicLong updateCount = new AtomicLong(0);
    private final AtomicLong deleteCount = new AtomicLong(0);
    private long startTime;
    private ScheduledExecutorService executorService;
    private Map<Integer, Map<String, Object>> lastSourceSnapshot = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        executorService = Executors.newScheduledThreadPool(2);
        // Send statistics every second
        executorService.scheduleAtFixedRate(this::sendStatistics, 0, 1, TimeUnit.SECONDS);
    }

    public void connect() throws Exception {
        if (isConnected.get()) {
            log.warn("CDC client is already connected");
            return;
        }

        isConnected.set(true);
        log.info("CDC client connected - ready to stream");
    }

    public void startStreaming(String database, List<String> tables) throws Exception {
        if (!isConnected.get()) {
            throw new IllegalStateException("CDC client is not connected");
        }

        if (isStreaming.get()) {
            log.warn("CDC streaming is already active");
            return;
        }

        isStreaming.set(true);
        startTime = System.currentTimeMillis();
        log.info("CDC streaming started for database: {}, tables: {}", database, tables);

        // Take initial snapshot and replicate existing records
        Map<Integer, Map<String, Object>> initialSnapshot = takeSnapshot();
        log.info("Initial snapshot contains {} records - replicating to target", initialSnapshot.size());

        // Replicate all existing records to target
        int eventsSent = 0;
        for (Map<String, Object> row : initialSnapshot.values()) {
            CDCEvent event = createInsertEvent(row);
            log.debug("Processing initial event: {}", event);
            processCDCEvent(event);
            eventsSent++;
        }
        log.info("Sent {} INSERT events via WebSocket for initial snapshot", eventsSent);

        lastSourceSnapshot = initialSnapshot;

        // Start polling for changes every 2 seconds
        executorService.scheduleAtFixedRate(this::pollForChanges, 2, 2, TimeUnit.SECONDS);
        log.info("Started polling for changes every 2 seconds");
    }

    public void stopStreaming() {
        if (isStreaming.get()) {
            isStreaming.set(false);
            lastSourceSnapshot.clear();
            log.info("CDC streaming stopped");
        }
    }

    private void pollForChanges() {
        if (!isStreaming.get()) {
            return;
        }

        try {
            Map<Integer, Map<String, Object>> currentSnapshot = takeSnapshot();
            
            // Detect changes
            for (Map.Entry<Integer, Map<String, Object>> entry : currentSnapshot.entrySet()) {
                int orderId = entry.getKey();
                Map<String, Object> currentRow = entry.getValue();
                Map<String, Object> oldRow = lastSourceSnapshot.get(orderId);
                
                if (oldRow == null) {
                    // INSERT - new record
                    processCDCEvent(createInsertEvent(currentRow));
                } else if (!oldRow.equals(currentRow)) {
                    // UPDATE - record changed
                    processCDCEvent(createUpdateEvent(oldRow, currentRow));
                }
            }
            
            // Detect deletes
            for (Integer orderId : lastSourceSnapshot.keySet()) {
                if (!currentSnapshot.containsKey(orderId)) {
                    // DELETE - record removed
                    processCDCEvent(createDeleteEvent(lastSourceSnapshot.get(orderId)));
                }
            }
            
            lastSourceSnapshot = currentSnapshot;
            
        } catch (Exception e) {
            log.error("Error polling for changes", e);
        }
    }

    private Map<Integer, Map<String, Object>> takeSnapshot() {
        Map<Integer, Map<String, Object>> snapshot = new HashMap<>();
        try (Connection conn = DriverManager.getConnection(sourceDbUrl, sourceDbUsername, sourceDbPassword);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT order_id, user_id, status FROM t_order")) {
            
            while (rs.next()) {
                int orderId = rs.getInt("order_id");
                Map<String, Object> row = new HashMap<>();
                row.put("order_id", orderId);
                row.put("user_id", rs.getInt("user_id"));
                row.put("status", rs.getString("status"));
                snapshot.put(orderId, row);
            }
        } catch (SQLException e) {
            log.error("Error taking snapshot", e);
        }
        return snapshot;
    }

    private CDCEvent createInsertEvent(Map<String, Object> afterData) {
        return CDCEvent.builder()
                .eventType("INSERT")
                .database("sharding_db")
                .tableName("t_order")
                .beforeData(Collections.emptyMap())
                .afterData(afterData)
                .data(afterData)
                .timestamp(System.currentTimeMillis())
                .build();
    }

    private CDCEvent createUpdateEvent(Map<String, Object> beforeData, Map<String, Object> afterData) {
        return CDCEvent.builder()
                .eventType("UPDATE")
                .database("sharding_db")
                .tableName("t_order")
                .beforeData(beforeData)
                .afterData(afterData)
                .data(afterData)
                .timestamp(System.currentTimeMillis())
                .build();
    }

    private CDCEvent createDeleteEvent(Map<String, Object> beforeData) {
        return CDCEvent.builder()
                .eventType("DELETE")
                .database("sharding_db")
                .tableName("t_order")
                .beforeData(beforeData)
                .afterData(Collections.emptyMap())
                .data(beforeData)
                .timestamp(System.currentTimeMillis())
                .build();
    }

    private void processCDCEvent(CDCEvent event) {
        totalEvents.incrementAndGet();
        
        switch (event.getEventType()) {
            case "INSERT":
                insertCount.incrementAndGet();
                applyInsert(event);
                break;
            case "UPDATE":
                updateCount.incrementAndGet();
                applyUpdate(event);
                break;
            case "DELETE":
                deleteCount.incrementAndGet();
                applyDelete(event);
                break;
        }
        
        // Send event via WebSocket
        if (webSocketService != null) {
            log.debug("Broadcasting CDC event via WebSocket: type={}, order_id={}", 
                event.getEventType(), event.getAfterData().get("order_id"));
            webSocketService.broadcast("cdcEvent", event);
        } else {
            log.error("WebSocketService is NULL! Cannot broadcast CDC event!");
        }
    }

    private void applyInsert(CDCEvent event) {
        Map<String, Object> data = event.getAfterData();
        String sql = "INSERT INTO t_order (order_id, user_id, status) VALUES (?, ?, ?) ON CONFLICT (order_id) DO NOTHING";
        
        try (Connection conn = DriverManager.getConnection(targetDbUrl, targetDbUsername, targetDbPassword);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setInt(1, (Integer) data.get("order_id"));
            pstmt.setInt(2, (Integer) data.get("user_id"));
            pstmt.setString(3, (String) data.get("status"));
            pstmt.executeUpdate();
            
            log.debug("Applied INSERT to target: order_id={}", data.get("order_id"));
        } catch (SQLException e) {
            log.error("Error applying INSERT", e);
        }
    }

    private void applyUpdate(CDCEvent event) {
        Map<String, Object> data = event.getAfterData();
        String sql = "UPDATE t_order SET user_id = ?, status = ? WHERE order_id = ?";
        
        try (Connection conn = DriverManager.getConnection(targetDbUrl, targetDbUsername, targetDbPassword);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setInt(1, (Integer) data.get("user_id"));
            pstmt.setString(2, (String) data.get("status"));
            pstmt.setInt(3, (Integer) data.get("order_id"));
            pstmt.executeUpdate();
            
            log.debug("Applied UPDATE to target: order_id={}", data.get("order_id"));
        } catch (SQLException e) {
            log.error("Error applying UPDATE", e);
        }
    }

    private void applyDelete(CDCEvent event) {
        Map<String, Object> data = event.getBeforeData();
        String sql = "DELETE FROM t_order WHERE order_id = ?";
        
        try (Connection conn = DriverManager.getConnection(targetDbUrl, targetDbUsername, targetDbPassword);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setInt(1, (Integer) data.get("order_id"));
            pstmt.executeUpdate();
            
            log.debug("Applied DELETE to target: order_id={}", data.get("order_id"));
        } catch (SQLException e) {
            log.error("Error applying DELETE", e);
        }
    }

    private void sendStatistics() {
        if (webSocketService != null) {
            webSocketService.broadcast("cdcStatistics", getStatistics());
        }
    }

    public void resetStatistics() {
        totalEvents.set(0);
        insertCount.set(0);
        updateCount.set(0);
        deleteCount.set(0);
        startTime = System.currentTimeMillis();
        log.info("CDC statistics reset");
    }

    public CDCStatistics getStatistics() {
        long uptime = isStreaming.get() ? System.currentTimeMillis() - startTime : 0;
        double eventsPerSecond = uptime > 0 ? (totalEvents.get() * 1000.0) / uptime : 0;
        
        return CDCStatistics.builder()
            .totalEvents(totalEvents.get())
            .insertCount(insertCount.get())
            .updateCount(updateCount.get())
            .deleteCount(deleteCount.get())
                .eventsPerSecond(eventsPerSecond)
            .uptime(uptime)
            .build();
    }
    
    public boolean isConnected() {
        return isConnected.get();
    }

    public boolean isStreaming() {
        return isStreaming.get();
    }

    @PreDestroy
    public void cleanup() {
        stopStreaming();
        if (executorService != null) {
            executorService.shutdownNow();
        }
        isConnected.set(false);
        log.info("CDC client closed");
    }
}
