package org.apache.shardingsphere.example.proxy.cdc.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.data.pipeline.cdc.client.CDCClient;
import org.apache.shardingsphere.data.pipeline.cdc.client.config.CDCClientConfiguration;
import org.apache.shardingsphere.data.pipeline.cdc.client.handler.RetryStreamingExceptionHandler;
import org.apache.shardingsphere.data.pipeline.cdc.client.parameter.CDCLoginParameter;
import org.apache.shardingsphere.data.pipeline.cdc.client.parameter.StartStreamingParameter;
import org.apache.shardingsphere.data.pipeline.cdc.protocol.request.StreamDataRequestBody.SchemaTable;
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
 * CDC Client Service using Real ShardingSphere CDC
 * This implementation uses the native ShardingSphere CDC Client API
 * to stream changes from ShardingSphere Proxy via WAL-based logical replication
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

    @Value("${cdc.username:root}")
    private String cdcUsername;

    @Value("${cdc.password:root}")
    private String cdcPassword;

    @Value("${target.datasource.url}")
    private String targetDbUrl;

    @Value("${target.datasource.username}")
    private String targetDbUsername;

    @Value("${target.datasource.password}")
    private String targetDbPassword;

    private CDCClient cdcClient;
    private String currentStreamingId;
    private String currentStreamingDatabase; // Track which database is being streamed
    private final AtomicBoolean isStreaming = new AtomicBoolean(false);
    private final AtomicBoolean isConnected = new AtomicBoolean(false);
    private final AtomicLong totalEvents = new AtomicLong(0);
    private final AtomicLong insertCount = new AtomicLong(0);
    private final AtomicLong updateCount = new AtomicLong(0);
    private final AtomicLong deleteCount = new AtomicLong(0);
    private long startTime;
    private ScheduledExecutorService executorService;

    @PostConstruct
    public void init() {
        executorService = Executors.newScheduledThreadPool(2);
        // Send statistics every second
        executorService.scheduleAtFixedRate(this::sendStatistics, 0, 1, TimeUnit.SECONDS);
        log.info("CDC Client Service initialized (Real ShardingSphere CDC)");
    }

    /**
     * Connect to ShardingSphere CDC Server
     */
    public void connect() throws Exception {
        if (isConnected.get()) {
            log.warn("CDC client is already connected");
            return;
        }

        try {
            log.info("Connecting to ShardingSphere CDC Server at {}:{}", serverAddress, serverPort);
            
            // Create CDC Client Configuration
            CDCClientConfiguration config = new CDCClientConfiguration(serverAddress, serverPort, 10000);
            cdcClient = new CDCClient(config);
            
            // Connect with record consumer, streaming exception handler, and server error handler
            cdcClient.connect(
                // Data consumer - process incoming CDC records
                records -> {
                    try {
                        processCDCRecords(records);
                    } catch (Exception e) {
                        log.error("Error processing CDC records", e);
                    }
                },
                // Streaming exception handler - retry on connection errors
                new RetryStreamingExceptionHandler(cdcClient, 5, 5000),
                // Server error handler
                (ctx, result) -> {
                    log.error("CDC Server error: {}", result.getErrorMessage());
                }
            );
            
            // Login to CDC Server
            cdcClient.login(new CDCLoginParameter(cdcUsername, cdcPassword));
            
            isConnected.set(true);
            log.info("Successfully connected to ShardingSphere CDC Server");
            
        } catch (Exception e) {
            log.error("Failed to connect to CDC Server", e);
            isConnected.set(false);
            throw new RuntimeException("Failed to connect to CDC Server: " + e.getMessage(), e);
        }
    }

    /**
     * Start streaming CDC events from ShardingSphere Proxy
     */
    public void startStreaming(String database, List<String> tables) throws Exception {
        if (!isConnected.get()) {
            throw new IllegalStateException("CDC client is not connected. Call connect() first.");
        }

        if (isStreaming.get()) {
            log.warn("CDC streaming is already active");
            return;
        }

        try {
            log.info("Starting CDC streaming for database: {}, tables: {}", database, tables);
            
            // Build schema table set
            Set<SchemaTable> schemaTables = new HashSet<>();
            for (String table : tables) {
                schemaTables.add(SchemaTable.newBuilder()
                    .setTable(table)
                    .build());
            }
            
            // Start streaming with full data synchronization enabled
            StartStreamingParameter parameter = new StartStreamingParameter(
                database,           // Database name (e.g., "sharding_db")
                schemaTables,       // Tables to stream
                true                // full: true = include initial snapshot, false = incremental only
            );
            
            currentStreamingId = cdcClient.startStreaming(parameter);
            currentStreamingDatabase = database; // Track which database is being streamed
            
            isStreaming.set(true);
            startTime = System.currentTimeMillis();
            
            log.info("CDC streaming started successfully. Streaming ID: {}, Database: {}", currentStreamingId, database);
            log.info("Now receiving real-time CDC events from ShardingSphere Proxy via WAL-based replication");
            
        } catch (Exception e) {
            log.error("Failed to start CDC streaming", e);
            isStreaming.set(false);
            throw new RuntimeException("Failed to start CDC streaming: " + e.getMessage(), e);
        }
    }

    /**
     * Stop CDC streaming
     */
    public void stopStreaming() {
        if (isStreaming.get()) {
            try {
                log.info("Stopping CDC streaming (ID: {}, Database: {})", currentStreamingId, currentStreamingDatabase);
    
                // IMPORTANT: Actually stop the CDC job at proxy level!
                if (currentStreamingId != null && cdcClient != null) {
                    cdcClient.stopStreaming(currentStreamingId);
                    log.info("Stopped CDC job at proxy: {}", currentStreamingId);
                }
                
                isStreaming.set(false);
                currentStreamingId = null;
                currentStreamingDatabase = null; // Clear tracked database
                
                log.info("CDC streaming stopped");
            } catch (Exception e) {
                log.error("Error stopping CDC streaming", e);
            }
        }
    }
    
    /**
     * Get the currently streaming database name
     */
    public String getCurrentStreamingDatabase() {
        return currentStreamingDatabase;
    }

    /**
     * Process incoming CDC records from ShardingSphere
     */
    private void processCDCRecords(Object records) {
        try {
            // ShardingSphere CDC sends DataRecordResult objects
            if (records == null) {
                log.warn("Received null CDC records");
                return;
            }
            
            java.util.List<Object> recordsList;
            
            // Check if records is already a List (ShardingSphere sometimes sends the list directly)
            if (records instanceof java.util.List) {
                @SuppressWarnings("unchecked")
                java.util.List<Object> list = (java.util.List<Object>) records;
                recordsList = list;
                log.debug("Received CDC records as direct list with {} items", recordsList.size());
            } else {
                // Otherwise, use reflection to get the records list
                Class<?> recordsClass = records.getClass();
                java.lang.reflect.Method getRecordsMethod = recordsClass.getMethod("getRecordsList");
                @SuppressWarnings("unchecked")
                java.util.List<Object> list = (java.util.List<Object>) getRecordsMethod.invoke(records);
                recordsList = list;
                log.debug("Received CDC records via getRecordsList() with {} items", recordsList.size());
            }
            
            for (Object record : recordsList) {
                processIndividualRecord(record);
            }
            
        } catch (Exception e) {
            log.error("Error processing CDC records", e);
        }
    }

    /**
     * Process individual CDC record using reflection to extract data
     */
    private void processIndividualRecord(Object record) {
        try {
            Class<?> recordClass = record.getClass();
            
            // Log available methods for debugging
            if (log.isDebugEnabled()) {
                log.debug("Record class: {}", recordClass.getName());
                log.debug("Available methods: {}", java.util.Arrays.toString(recordClass.getMethods()));
            }
            
            // Get operation type
            java.lang.reflect.Method getDataChangeTypeMethod = recordClass.getMethod("getDataChangeType");
            Object dataChangeType = getDataChangeTypeMethod.invoke(record);
            String operation = dataChangeType.toString(); // INSERT, UPDATE, DELETE
            
            // Get metadata which contains table name
            java.lang.reflect.Method getMetaDataMethod = recordClass.getMethod("getMetaData");
            Object metaData = getMetaDataMethod.invoke(record);
            
            java.lang.reflect.Method getTableMethod = metaData.getClass().getMethod("getTable");
            String tableName = (String) getTableMethod.invoke(metaData);
            
            log.debug("Processing CDC record: operation={}, table={}", operation, tableName);
            
            // Get before and after data
            java.lang.reflect.Method getBeforeMethod = recordClass.getMethod("getBeforeList");
            java.lang.reflect.Method getAfterMethod = recordClass.getMethod("getAfterList");
            
            @SuppressWarnings("unchecked")
            java.util.List<Object> beforeList = (java.util.List<Object>) getBeforeMethod.invoke(record);
            @SuppressWarnings("unchecked")
            java.util.List<Object> afterList = (java.util.List<Object>) getAfterMethod.invoke(record);
            
            Map<String, Object> beforeData = extractDataFromColumns(beforeList);
            Map<String, Object> afterData = extractDataFromColumns(afterList);
            
            // Create CDCEvent
            CDCEvent event = CDCEvent.builder()
                .eventType(operation)
                .database("sharding_db")
                .tableName(tableName)
                .beforeData(beforeData)
                .afterData(afterData)
                .data(afterData.isEmpty() ? beforeData : afterData)
                .timestamp(System.currentTimeMillis())
                .build();
            
            processCDCEvent(event);
            
        } catch (Exception e) {
            log.error("Error processing individual CDC record", e);
        }
    }
    
    /**
     * Extract data from column list using reflection
     * Properly handles Google Protobuf Any wrapper types (Int32Value, StringValue, etc.)
     */
    private Map<String, Object> extractDataFromColumns(java.util.List<Object> columnList) {
        Map<String, Object> data = new HashMap<>();
        
        try {
            for (Object column : columnList) {
                Class<?> columnClass = column.getClass();
                
                // Get column name
                java.lang.reflect.Method getNameMethod = columnClass.getMethod("getName");
                String columnName = (String) getNameMethod.invoke(column);
                
                // Get column value - this is a Protobuf Any object
                java.lang.reflect.Method getValueMethod = columnClass.getMethod("getValue");
                Object anyValue = getValueMethod.invoke(column);
                
                if (anyValue != null) {
                    // Extract the actual value from the Protobuf Any wrapper
                    Object extractedValue = extractFromProtobufAny(anyValue);
                    data.put(columnName, extractedValue);
                }
            }
        } catch (Exception e) {
            log.error("Error extracting data from columns", e);
        }
        
        return data;
    }
    
    /**
     * Extract actual value from Google Protobuf Any wrapper
     * Handles Int32Value, Int64Value, StringValue, BoolValue, Timestamp, etc.
     */
    private Object extractFromProtobufAny(Object anyObject) {
        try {
            Class<?> anyClass = anyObject.getClass();
            
            // Get the type URL to determine what kind of value this is
            java.lang.reflect.Method getTypeUrlMethod = anyClass.getMethod("getTypeUrl");
            String typeUrl = (String) getTypeUrlMethod.invoke(anyObject);
            
            // Get the value bytes
            java.lang.reflect.Method getValueMethod = anyClass.getMethod("getValue");
            Object valueBytes = getValueMethod.invoke(anyObject);
            
            // Parse based on type
            if (typeUrl.contains("Int32Value")) {
                // Parse Int32Value from ByteString
                return parseInt32Value(valueBytes);
            } else if (typeUrl.contains("Int64Value")) {
                return parseInt64Value(valueBytes);
            } else if (typeUrl.contains("StringValue")) {
                return parseStringValue(valueBytes);
            } else if (typeUrl.contains("BoolValue")) {
                return parseBoolValue(valueBytes);
            } else if (typeUrl.contains("Timestamp")) {
                // For now, return timestamp as string
                return parseTimestamp(valueBytes);
            } else {
                // Fallback: return as string
                return valueBytes.toString();
            }
            
        } catch (Exception e) {
            log.error("Error extracting from Protobuf Any", e);
            return null;
        }
    }
    
    /**
     * Parse Int32Value from Protobuf ByteString
     */
    private Integer parseInt32Value(Object byteStringObj) {
        try {
            // ByteString.toByteArray()
            java.lang.reflect.Method toByteArrayMethod = byteStringObj.getClass().getMethod("toByteArray");
            byte[] bytes = (byte[]) toByteArrayMethod.invoke(byteStringObj);
            
            // Varint decoding: simple case for small integers
            // Format: tag (field 1, wiretype 0) + varint value
            if (bytes.length >= 2 && bytes[0] == 0x08) { // Field 1, wiretype 0
                // Decode varint
                int value = 0;
                int shift = 0;
                for (int i = 1; i < bytes.length; i++) {
                    value |= (bytes[i] & 0x7F) << shift;
                    if ((bytes[i] & 0x80) == 0) break;
                    shift += 7;
                }
                return value;
            }
            
            log.warn("Unable to parse Int32Value from bytes");
            return null;
        } catch (Exception e) {
            log.error("Error parsing Int32Value", e);
            return null;
        }
    }
    
    /**
     * Parse Int64Value from Protobuf ByteString
     */
    private Long parseInt64Value(Object byteStringObj) {
        try {
            java.lang.reflect.Method toByteArrayMethod = byteStringObj.getClass().getMethod("toByteArray");
            byte[] bytes = (byte[]) toByteArrayMethod.invoke(byteStringObj);
            
            if (bytes.length >= 2 && bytes[0] == 0x08) {
                long value = 0;
                int shift = 0;
                for (int i = 1; i < bytes.length; i++) {
                    value |= (long)(bytes[i] & 0x7F) << shift;
                    if ((bytes[i] & 0x80) == 0) break;
                    shift += 7;
                }
                return value;
            }
            
            return null;
        } catch (Exception e) {
            log.error("Error parsing Int64Value", e);
            return null;
        }
    }
    
    /**
     * Parse StringValue from Protobuf ByteString
     */
    private String parseStringValue(Object byteStringObj) {
        try {
            java.lang.reflect.Method toByteArrayMethod = byteStringObj.getClass().getMethod("toByteArray");
            byte[] bytes = (byte[]) toByteArrayMethod.invoke(byteStringObj);
            
            // String format: tag (field 1, wiretype 2) + length + UTF-8 bytes
            if (bytes.length >= 3 && bytes[0] == 0x0A) { // Field 1, wiretype 2
                int length = bytes[1] & 0xFF;
                return new String(bytes, 2, length, "UTF-8");
            }
            
            return null;
        } catch (Exception e) {
            log.error("Error parsing StringValue", e);
            return null;
        }
    }
    
    /**
     * Parse BoolValue from Protobuf ByteString
     */
    private Boolean parseBoolValue(Object byteStringObj) {
        try {
            java.lang.reflect.Method toByteArrayMethod = byteStringObj.getClass().getMethod("toByteArray");
            byte[] bytes = (byte[]) toByteArrayMethod.invoke(byteStringObj);
            
            if (bytes.length >= 2 && bytes[0] == 0x08) {
                return bytes[1] != 0;
            }
            
            return null;
        } catch (Exception e) {
            log.error("Error parsing BoolValue", e);
            return null;
        }
    }
    
    /**
     * Parse Timestamp from Protobuf ByteString (return as string for now)
     */
    private String parseTimestamp(Object byteStringObj) {
        try {
            // For now, return a simple representation
            // Full timestamp parsing would require decoding seconds and nanos fields
            return "timestamp";
        } catch (Exception e) {
            log.error("Error parsing Timestamp", e);
            return null;
        }
    }

    /**
     * Parse CDC record from ShardingSphere and convert to our CDCEvent format
     * @deprecated Use processIndividualRecord instead
     */
    @Deprecated
    private void parseCDCRecord(String recordStr) {
        try {
            // Parse the record string to extract event details
            // This is a simplified parser - in production you'd use the protobuf objects directly
            
            String eventType = null;
            Map<String, Object> beforeData = new HashMap<>();
            Map<String, Object> afterData = new HashMap<>();
            
            if (recordStr.contains("INSERT")) {
                eventType = "INSERT";
                afterData = extractDataFromRecord(recordStr, "after");
            } else if (recordStr.contains("UPDATE")) {
                eventType = "UPDATE";
                beforeData = extractDataFromRecord(recordStr, "before");
                afterData = extractDataFromRecord(recordStr, "after");
            } else if (recordStr.contains("DELETE")) {
                eventType = "DELETE";
                beforeData = extractDataFromRecord(recordStr, "before");
            }
            
            if (eventType != null) {
                CDCEvent event = CDCEvent.builder()
                    .eventType(eventType)
                    .database("sharding_db")
                    .tableName("t_order")
                    .beforeData(beforeData)
                    .afterData(afterData)
                    .data(afterData.isEmpty() ? beforeData : afterData)
                    .timestamp(System.currentTimeMillis())
                    .build();
                
                processCDCEvent(event);
            }
            
        } catch (Exception e) {
            log.error("Error parsing CDC record", e);
        }
    }

    /**
     * Extract data fields from CDC record string
     */
    private Map<String, Object> extractDataFromRecord(String recordStr, String section) {
        Map<String, Object> data = new HashMap<>();
        
        try {
            // Simple parser for protobuf string format
            // In production, use proper protobuf deserialization
            
            int sectionStart = recordStr.indexOf(section + " {");
            if (sectionStart == -1) return data;
            
            int sectionEnd = recordStr.indexOf("}", sectionStart);
            String sectionContent = recordStr.substring(sectionStart, sectionEnd);
            
            // Extract order_id
            if (sectionContent.contains("order_id")) {
                String orderIdStr = extractValue(sectionContent, "order_id");
                if (orderIdStr != null) {
                    data.put("order_id", Integer.parseInt(orderIdStr.trim()));
                }
            }
            
            // Extract user_id
            if (sectionContent.contains("user_id")) {
                String userIdStr = extractValue(sectionContent, "user_id");
                if (userIdStr != null) {
                    data.put("user_id", Integer.parseInt(userIdStr.trim()));
                }
            }
            
            // Extract status
            if (sectionContent.contains("status")) {
                String statusStr = extractValue(sectionContent, "status");
                if (statusStr != null) {
                    data.put("status", statusStr.trim().replace("\"", ""));
                }
            }
            
        } catch (Exception e) {
            log.warn("Error extracting data from record section: {}", section, e);
        }
        
        return data;
    }

    /**
     * Extract a value for a given field name from the record string
     */
    private String extractValue(String content, String fieldName) {
        try {
            int fieldStart = content.indexOf(fieldName);
            if (fieldStart == -1) return null;
            
            int valueStart = content.indexOf(":", fieldStart);
            if (valueStart == -1) return null;
            
            int valueEnd = content.indexOf("\n", valueStart);
            if (valueEnd == -1) valueEnd = content.length();
            
            return content.substring(valueStart + 1, valueEnd).trim();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Process a CDC event (apply to target and broadcast to UI)
     */
    private void processCDCEvent(CDCEvent event) {
        if (event == null || event.getEventType() == null) {
            log.warn("Received null or invalid CDC event");
            return;
        }
        
        totalEvents.incrementAndGet();
        
        log.info("Processing CDC event: type={}, table={}, data={}", 
            event.getEventType(), event.getTableName(), event.getData());
        
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
            default:
                log.warn("Unknown event type: {}", event.getEventType());
        }
        
        // Send event via WebSocket
        if (webSocketService != null) {
            Object orderId = event.getAfterData().isEmpty() ? 
                event.getBeforeData().get("order_id") : 
                event.getAfterData().get("order_id");
            log.debug("Broadcasting CDC event via WebSocket: type={}, order_id={}", 
                event.getEventType(), orderId);
            webSocketService.broadcast("cdcEvent", event);
        } else {
            log.error("WebSocketService is NULL! Cannot broadcast CDC event!");
        }
    }

    /**
     * Apply INSERT event to target database
     */
    private void applyInsert(CDCEvent event) {
        Map<String, Object> data = event.getAfterData();
        
        if (data == null || data.isEmpty()) {
            log.warn("Cannot apply INSERT: afterData is empty");
            return;
        }
        
        String sql = "INSERT INTO t_order (order_id, user_id, status) VALUES (?, ?, ?) ON CONFLICT (order_id) DO NOTHING";
        
        try (Connection conn = DriverManager.getConnection(targetDbUrl, targetDbUsername, targetDbPassword);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            Object orderId = data.get("order_id");
            Object userId = data.get("user_id");
            Object status = data.get("status");
            
            if (orderId == null || userId == null) {
                log.warn("Missing required fields for INSERT: order_id={}, user_id={}", orderId, userId);
                return;
            }
            
            pstmt.setInt(1, orderId instanceof Integer ? (Integer) orderId : Integer.parseInt(orderId.toString()));
            pstmt.setInt(2, userId instanceof Integer ? (Integer) userId : Integer.parseInt(userId.toString()));
            pstmt.setString(3, status != null ? status.toString() : "");
            
            int rowsAffected = pstmt.executeUpdate();
            log.info("✅ Applied INSERT to target: order_id={}, user_id={}, status={}, rows={}", 
                orderId, userId, status, rowsAffected);
                
        } catch (SQLException e) {
            log.error("❌ Error applying INSERT: data={}", data, e);
        } catch (NumberFormatException e) {
            log.error("❌ Error parsing numeric fields: data={}", data, e);
        }
    }

    /**
     * Apply UPDATE event to target database
     */
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

    /**
     * Apply DELETE event to target database
     */
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

    /**
     * Send statistics to WebSocket clients
     */
    private void sendStatistics() {
        if (webSocketService != null) {
            webSocketService.broadcast("cdcStatistics", getStatistics());
        }
    }

    /**
     * Reset CDC statistics
     */
    public void resetStatistics() {
        totalEvents.set(0);
        insertCount.set(0);
        updateCount.set(0);
        deleteCount.set(0);
        startTime = System.currentTimeMillis();
        log.info("CDC statistics reset");
    }

    /**
     * Get current CDC statistics
     */
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
    
    /**
     * Check if CDC client is connected
     */
    public boolean isConnected() {
        return isConnected.get();
    }

    /**
     * Check if CDC streaming is active
     */
    public boolean isStreaming() {
        return isStreaming.get();
    }

    /**
     * Cleanup resources on shutdown
     */
    @PreDestroy
    public void cleanup() {
        try {
            stopStreaming();
            
            if (cdcClient != null) {
                // Close the CDC client connection
                cdcClient.close();
                log.info("CDC client closed");
            }
            
            if (executorService != null) {
                executorService.shutdownNow();
            }
            
            isConnected.set(false);
            isStreaming.set(false);
            
        } catch (Exception e) {
            log.error("Error during cleanup", e);
        }
    }
}
