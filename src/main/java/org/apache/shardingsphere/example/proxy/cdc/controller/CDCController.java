package org.apache.shardingsphere.example.proxy.cdc.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.example.proxy.cdc.model.CDCStatistics;
import org.apache.shardingsphere.example.proxy.cdc.model.CDCStatus;
import org.apache.shardingsphere.example.proxy.cdc.service.CDCClientService;
import org.apache.shardingsphere.example.proxy.cdc.service.DataGeneratorService;
import org.apache.shardingsphere.example.proxy.cdc.service.ReplicationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * CDC REST Controller
 */
@Slf4j
@RestController
@RequestMapping("/api/cdc")
@CrossOrigin(origins = "*")
public class CDCController {

    @Autowired
    private CDCClientService cdcClientService;

    @Autowired
    private DataGeneratorService dataGeneratorService;

    @Autowired
    private ReplicationService replicationService;

    @Autowired
    private org.apache.shardingsphere.example.proxy.cdc.service.MigrationService migrationService;

    @PostConstruct
    public void init() {
        try {
            // Auto-connect on startup
            cdcClientService.connect();
            log.info("CDC client auto-connected on startup");
        } catch (Exception e) {
            log.error("Failed to auto-connect CDC client on startup", e);
        }
    }

    @GetMapping("/status")
    public ResponseEntity<CDCStatus> getStatus() {
        try {
            boolean connected = cdcClientService.isConnected();
            boolean streaming = cdcClientService.isStreaming();
            String message = connected ? 
                (streaming ? "Streaming active" : "Connected, not streaming") :
                "Not connected";

            CDCStatus status = CDCStatus.builder()
                    .connected(connected)
                    .streaming(streaming)
                    .message(message)
                    .build();

            return ResponseEntity.ok(status);
        } catch (Exception e) {
            log.error("Error getting CDC status", e);
            return ResponseEntity.ok(CDCStatus.builder()
                    .connected(false)
                    .streaming(false)
                    .message("Error: " + e.getMessage())
                    .build());
        }
    }

    /**
     * Get detailed status of all CDC streams
     */
    @GetMapping("/streams/status")
    public ResponseEntity<Map<String, Object>> getStreamsStatus() {
        try {
            boolean connected = cdcClientService.isConnected();
            boolean streaming = cdcClientService.isStreaming();
            String currentDatabase = cdcClientService.getCurrentStreamingDatabase();
            
            // Determine which CDC stream is active based on the database being monitored
            boolean shardingCdcActive = streaming && "sharding_db".equals(currentDatabase);
            boolean migrationCdcActive = streaming && "migration_db".equals(currentDatabase);
            
            Map<String, Object> status = new HashMap<>();
            status.put("connected", connected);
            status.put("shardingCdcActive", shardingCdcActive);
            status.put("migrationCdcActive", migrationCdcActive);
            status.put("currentDatabase", currentDatabase); // For debugging
            
            log.debug("CDC Streams Status - Connected: {}, Streaming: {}, Database: {}, ShardingActive: {}, MigrationActive: {}", 
                connected, streaming, currentDatabase, shardingCdcActive, migrationCdcActive);
            
            return ResponseEntity.ok(status);
        } catch (Exception e) {
            log.error("Error getting streams status", e);
            return ResponseEntity.status(500).body(Map.of(
                "connected", false,
                "shardingCdcActive", false,
                "migrationCdcActive", false,
                "error", e.getMessage()
            ));
        }
    }

    @PostMapping("/connect")
    public ResponseEntity<Map<String, Object>> connect() {
        try {
            cdcClientService.connect();
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "CDC client connected successfully"
            ));
        } catch (Exception e) {
            log.error("Error connecting CDC client", e);
            return ResponseEntity.status(500)
                    .body(Map.of("success", false, "message", e.getMessage()));
        }
    }

    @PostMapping("/startStreaming")
    public ResponseEntity<String> startStreaming(
            @RequestParam(defaultValue = "sharding_db") String database,
            @RequestParam(defaultValue = "t_order") String tables) {
        try {
            List<String> tableList = Arrays.asList(tables.split(","));
            cdcClientService.startStreaming(database, tableList);
            return ResponseEntity.ok("CDC streaming started successfully");
        } catch (Exception e) {
            log.error("Error starting CDC streaming", e);
            return ResponseEntity.status(500)
                    .body("Failed to start streaming: " + e.getMessage());
        }
    }

    @PostMapping("/stopStreaming")
    public ResponseEntity<String> stopStreaming() {
        try {
            cdcClientService.stopStreaming();
            return ResponseEntity.ok("CDC streaming stopped successfully");
        } catch (Exception e) {
            log.error("Error stopping CDC streaming", e);
            return ResponseEntity.status(500)
                    .body("Failed to stop streaming: " + e.getMessage());
        }
    }

    @GetMapping("/statistics")
    public ResponseEntity<CDCStatistics> getStatistics() {
        try {
            CDCStatistics stats = cdcClientService.getStatistics();
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            log.error("Error getting statistics", e);
            return ResponseEntity.status(500).build();
        }
    }

    @PostMapping("/generate")
    public ResponseEntity<Map<String, String>> generateData(
            @RequestParam(defaultValue = "10") int count,
            @RequestParam(defaultValue = "sharding_db") String database) {
        try {
            dataGeneratorService.generateData(count, database);
            return ResponseEntity.ok(Map.of("message", count + " records generated to " + database));
        } catch (Exception e) {
            log.error("Error generating data", e);
            return ResponseEntity.status(500)
                    .body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/generateData")
    public ResponseEntity<String> generateDataLegacy() {
        try {
            dataGeneratorService.generateData(1);
            return ResponseEntity.ok("Data generated successfully.");
        } catch (Exception e) {
            log.error("Error generating data", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to generate data: " + e.getMessage());
        }
    }

    @PostMapping("/reset")
    public ResponseEntity<Map<String, Object>> resetStatistics(
            @RequestParam(defaultValue = "false") boolean clearSource,
            @RequestParam(defaultValue = "false") boolean clearTarget) {
        try {
            // Reset CDC statistics
            cdcClientService.resetStatistics();
            
            // Clear source data if requested
            if (clearSource) {
                dataGeneratorService.clearSourceData();
                log.info("Source data cleared");
            }
            
            // Clear target data if requested
            if (clearTarget) {
                dataGeneratorService.clearTargetData();
                log.info("Target data cleared");
            }
            
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "Statistics reset successfully"
            ));
        } catch (Exception e) {
            log.error("Error resetting statistics", e);
            return ResponseEntity.status(500).body(Map.of(
                "success", false,
                "message", "Error: " + e.getMessage()
            ));
        }
    }

    /**
     * Clear ShardingSphere source database only
     */
    @PostMapping("/clearShardingSphereDb")
    public ResponseEntity<Map<String, Object>> clearShardingSphereDb() {
        try {
            dataGeneratorService.clearSourceData();
            log.info("ShardingSphere DB (source) cleared");
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "ShardingSphere DB cleared successfully"
            ));
        } catch (Exception e) {
            log.error("Error clearing ShardingSphere DB", e);
            return ResponseEntity.status(500).body(Map.of(
                "success", false,
                "message", "Error: " + e.getMessage()
            ));
        }
    }

    /**
     * Clear migration source database only
     */
    @PostMapping("/clearMigrationSourceDb")
    public ResponseEntity<Map<String, Object>> clearMigrationSourceDb() {
        try {
            migrationService.clearSourceDatabase();
            log.info("Migration Source DB cleared");
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "Migration Source DB cleared successfully"
            ));
        } catch (Exception e) {
            log.error("Error clearing Migration Source DB", e);
            return ResponseEntity.status(500).body(Map.of(
                "success", false,
                "message", "Error: " + e.getMessage()
            ));
        }
    }

    /**
     * Clear target database only
     */
    @PostMapping("/clearTargetDb")
    public ResponseEntity<Map<String, Object>> clearTargetDb() {
        try {
            dataGeneratorService.clearTargetData();
            log.info("Target DB cleared");
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "Target DB cleared successfully"
            ));
        } catch (Exception e) {
            log.error("Error clearing Target DB", e);
            return ResponseEntity.status(500).body(Map.of(
                "success", false,
                "message", "Error: " + e.getMessage()
            ));
        }
    }

    @PostMapping("/update/{orderId}")
    public ResponseEntity<Map<String, String>> updateData(
            @PathVariable int orderId,
            @RequestParam String status) {
        try {
            dataGeneratorService.updateData(orderId, status);
            return ResponseEntity.ok(Map.of("message", "Record updated"));
        } catch (Exception e) {
            log.error("Error updating data", e);
            return ResponseEntity.status(500)
                    .body(Map.of("error", e.getMessage()));
        }
    }

    @DeleteMapping("/delete/{orderId}")
    public ResponseEntity<Map<String, String>> deleteData(@PathVariable int orderId) {
        try {
            dataGeneratorService.deleteData(orderId);
            return ResponseEntity.ok(Map.of("message", "Record deleted"));
        } catch (Exception e) {
            log.error("Error deleting data", e);
            return ResponseEntity.status(500)
                    .body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/clearSource")
    public ResponseEntity<Map<String, String>> clearSourceData() {
        try {
            dataGeneratorService.clearSourceData();
            return ResponseEntity.ok(Map.of("message", "Source data cleared"));
        } catch (Exception e) {
            log.error("Error clearing source data", e);
            return ResponseEntity.status(500)
                    .body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/clearTarget")
    public ResponseEntity<Map<String, String>> clearTargetData() {
        try {
            dataGeneratorService.clearTargetData();
            return ResponseEntity.ok(Map.of("message", "Target data cleared"));
        } catch (Exception e) {
            log.error("Error clearing target data", e);
            return ResponseEntity.status(500)
                    .body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/sourceData")
    public ResponseEntity<Map<String, Object>> getSourceData() {
        try {
            List<Map<String, Object>> data = dataGeneratorService.getSourceData();
            return ResponseEntity.ok(Map.of("success", true, "data", data));
        } catch (Exception e) {
            log.error("Error getting source data", e);
            return ResponseEntity.ok(Map.of("success", false, "message", e.getMessage(), "data", List.of()));
        }
    }

    @GetMapping("/targetData")
    public ResponseEntity<Map<String, Object>> getTargetData() {
        try {
            List<Map<String, Object>> data = dataGeneratorService.getTargetData();
            return ResponseEntity.ok(Map.of("success", true, "data", data));
        } catch (Exception e) {
            log.error("Error getting target data", e);
            return ResponseEntity.ok(Map.of("success", false, "message", e.getMessage(), "data", List.of()));
        }
    }

    @GetMapping("/sourceRecordCount")
    public ResponseEntity<Map<String, Object>> getSourceRecordCount() {
        try {
            long count = dataGeneratorService.getSourceRecordCount();
            return ResponseEntity.ok(Map.of("success", true, "count", count));
        } catch (Exception e) {
            log.error("Error getting source record count", e);
            return ResponseEntity.ok(Map.of("success", false, "count", 0L, "message", e.getMessage()));
        }
    }

    @GetMapping("/targetRecordCount")
    public ResponseEntity<Map<String, Object>> getTargetRecordCount() {
        try {
            long count = dataGeneratorService.getTargetRecordCount();
            return ResponseEntity.ok(Map.of("success", true, "count", count));
        } catch (Exception e) {
            log.error("Error getting target record count", e);
            return ResponseEntity.ok(Map.of("success", false, "count", 0L, "message", e.getMessage()));
        }
    }

    @GetMapping("/recordCount")
    public ResponseEntity<Map<String, Object>> getTotalRecordCount() {
        try {
            long sourceCount = dataGeneratorService.getSourceRecordCount();
            return ResponseEntity.ok(Map.of("success", true, "count", sourceCount));
        } catch (Exception e) {
            log.error("Error getting record count", e);
            return ResponseEntity.ok(Map.of("success", false, "count", 0));
        }
    }

    @GetMapping("/generator/status")
    public ResponseEntity<Map<String, Object>> getGeneratorStatus() {
        return ResponseEntity.ok(Map.of(
            "running", dataGeneratorService.isGeneratorRunning(),
            "interval", dataGeneratorService.getGeneratorInterval()
        ));
    }

    @PostMapping("/generator/start")
    public ResponseEntity<Map<String, Object>> startGenerator(
            @RequestParam(defaultValue = "1000") int interval,
            @RequestParam(defaultValue = "MIXED") String operation,
            @RequestParam(defaultValue = "sharding_db") String database) {
        try {
            dataGeneratorService.startGenerator(interval, operation, database);
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "Data generator started for " + database + " with interval " + interval + "ms"
            ));
        } catch (Exception e) {
            log.error("Error starting generator", e);
            return ResponseEntity.status(500).body(Map.of(
                "success", false,
                "message", "Error: " + e.getMessage()
            ));
        }
    }

    @PostMapping("/generator/stop")
    public ResponseEntity<Map<String, Object>> stopGenerator() {
        try {
            dataGeneratorService.stopGenerator();
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "Data generator stopped"
            ));
        } catch (Exception e) {
            log.error("Error stopping generator", e);
            return ResponseEntity.status(500).body(Map.of(
                "success", false,
                "message", "Error: " + e.getMessage()
            ));
        }
    }

    @GetMapping("/events")
    public ResponseEntity<List<Map<String, Object>>> getCdcEvents(@RequestParam(defaultValue = "50") int limit) {
        // Return empty list for now - CDC events are streamed via WebSocket
        return ResponseEntity.ok(List.of());
    }

    @PostMapping("/generator/insert")
    public ResponseEntity<Map<String, Object>> generatorInsert(
            @RequestParam(required = false) Integer userId,
            @RequestParam(required = false) String status) {
        try {
            dataGeneratorService.generateData(1);
            return ResponseEntity.ok(Map.of("success", true, "message", "Order inserted successfully"));
        } catch (Exception e) {
            log.error("Error generating data", e);
            return ResponseEntity.status(500).body(Map.of("success", false, "message", "Error: " + e.getMessage()));
        }
    }

    @PostMapping("/generator/update")
    public ResponseEntity<Map<String, String>> generatorUpdate() {
        return ResponseEntity.ok(Map.of("message", "Update not implemented"));
    }

    @PostMapping("/generator/delete")
    public ResponseEntity<Map<String, String>> generatorDelete() {
        return ResponseEntity.ok(Map.of("message", "Delete not implemented"));
    }

    // ========== Read-Write Splitting Endpoints ==========

    @PostMapping("/replication/enable")
    public ResponseEntity<Map<String, Object>> enableReplication() {
        try {
            replicationService.enableReplication();
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "Replication enabled successfully"
            ));
        } catch (Exception e) {
            log.error("Error enabling replication", e);
            return ResponseEntity.status(500).body(Map.of(
                "success", false,
                "message", "Error: " + e.getMessage()
            ));
        }
    }

    @PostMapping("/replication/disable")
    public ResponseEntity<Map<String, Object>> disableReplication() {
        try {
            replicationService.disableReplication();
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "Replication disabled successfully"
            ));
        } catch (Exception e) {
            log.error("Error disabling replication", e);
            return ResponseEntity.status(500).body(Map.of(
                "success", false,
                "message", "Error: " + e.getMessage()
            ));
        }
    }

    @GetMapping("/replication/status")
    public ResponseEntity<Map<String, Object>> getReplicationStatus() {
        try {
            boolean enabled = replicationService.isReplicationEnabled();
            long writeCount = replicationService.getWriteRecordCount();
            long readCount = replicationService.getReadRecordCount();
            
            return ResponseEntity.ok(Map.of(
                "enabled", enabled,
                "writeRecordCount", writeCount,
                "readRecordCount", readCount,
                "inSync", writeCount == readCount
            ));
        } catch (Exception e) {
            log.error("Error getting replication status", e);
            return ResponseEntity.status(500).build();
        }
    }

    @PostMapping("/replication/sync")
    public ResponseEntity<Map<String, Object>> syncReplication() {
        try {
            replicationService.manualSync();
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "Manual sync completed successfully"
            ));
        } catch (Exception e) {
            log.error("Error syncing replication", e);
            return ResponseEntity.status(500).body(Map.of(
                "success", false,
                "message", "Error: " + e.getMessage()
            ));
        }
    }

    @PostMapping("/replication/switch")
    public ResponseEntity<Map<String, Object>> switchDatabases() {
        try {
            replicationService.switchDatabases();
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "Database roles switched successfully (write <-> read)"
            ));
        } catch (Exception e) {
            log.error("Error switching databases", e);
            return ResponseEntity.status(500).body(Map.of(
                "success", false,
                "message", "Error: " + e.getMessage()
            ));
        }
    }

    @GetMapping("/writeData")
    public ResponseEntity<Map<String, Object>> getWriteData() {
        try {
            List<Map<String, Object>> data = replicationService.getWriteData();
            return ResponseEntity.ok(Map.of("success", true, "data", data));
        } catch (Exception e) {
            log.error("Error getting write data", e);
            return ResponseEntity.ok(Map.of("success", false, "message", e.getMessage(), "data", List.of()));
        }
    }

    @GetMapping("/readData")
    public ResponseEntity<Map<String, Object>> getReadData() {
        try {
            List<Map<String, Object>> data = replicationService.getReadData();
            return ResponseEntity.ok(Map.of("success", true, "data", data));
        } catch (Exception e) {
            log.error("Error getting read data", e);
            return ResponseEntity.ok(Map.of("success", false, "message", e.getMessage(), "data", List.of()));
        }
    }

    @GetMapping("/writeRecordCount")
    public ResponseEntity<Map<String, Long>> getWriteRecordCount() {
        try {
            long count = replicationService.getWriteRecordCount();
            return ResponseEntity.ok(Map.of("count", count));
        } catch (Exception e) {
            log.error("Error getting write record count", e);
            return ResponseEntity.status(500).build();
        }
    }

    @GetMapping("/readRecordCount")
    public ResponseEntity<Map<String, Long>> getReadRecordCount() {
        try {
            long count = replicationService.getReadRecordCount();
            return ResponseEntity.ok(Map.of("count", count));
        } catch (Exception e) {
            log.error("Error getting read record count", e);
            return ResponseEntity.status(500).build();
        }
    }
    
    @PostMapping("/replication/insertWrite")
    public ResponseEntity<Map<String, Object>> insertToWriteDb(
            @RequestParam int userId,
            @RequestParam String status) {
        try {
            replicationService.insertToWriteDb(userId, status);
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "Record inserted to write database successfully"
            ));
        } catch (Exception e) {
            log.error("Error inserting to write database", e);
            return ResponseEntity.status(500).body(Map.of(
                "success", false,
                "message", "Error: " + e.getMessage()
            ));
        }
    }
    
    @GetMapping("/replication/searchRead")
    public ResponseEntity<Map<String, Object>> searchReadDatabase(
            @RequestParam(required = false) Integer order_id,
            @RequestParam(required = false) Integer user_id) {
        try {
            String searchType;
            int searchValue;
            
            if (order_id != null) {
                searchType = "order_id";
                searchValue = order_id;
            } else if (user_id != null) {
                searchType = "user_id";
                searchValue = user_id;
            } else {
                return ResponseEntity.ok(Map.of(
                    "success", false,
                    "message", "Please provide either order_id or user_id parameter",
                    "data", List.of()
                ));
            }
            
            List<Map<String, Object>> results = replicationService.searchReadDatabase(searchType, searchValue);
            return ResponseEntity.ok(Map.of(
                "success", true,
                "data", results,
                "message", "Found " + results.size() + " record(s) in read database"
            ));
        } catch (Exception e) {
            log.error("Error searching read database", e);
            return ResponseEntity.status(500).body(Map.of(
                "success", false,
                "message", "Error: " + e.getMessage(),
                "data", List.of()
            ));
        }
    }

    // ==================== Migration Endpoints ====================

    /**
     * Setup source database with sample data
     */
    @PostMapping("/migration/setup")
    public ResponseEntity<Map<String, Object>> setupMigrationSource() {
        try {
            log.info("Setting up migration source database");
            Map<String, Object> result = migrationService.setupSourceDatabase();
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("Error setting up migration source", e);
            return ResponseEntity.status(500).body(Map.of(
                "success", false,
                "message", "Error: " + e.getMessage()
            ));
        }
    }

    /**
     * Prepare target database
     */
    @PostMapping("/migration/prepare")
    public ResponseEntity<Map<String, Object>> prepareMigrationTarget() {
        try {
            log.info("Preparing migration target database");
            Map<String, Object> result = migrationService.prepareTargetDatabase();
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("Error preparing migration target", e);
            return ResponseEntity.status(500).body(Map.of(
                "success", false,
                "message", "Error: " + e.getMessage()
            ));
        }
    }

    /**
     * Copy initial data from source to target (baseline before CDC)
     */
    @PostMapping("/migration/copyInitialData")
    public ResponseEntity<Map<String, Object>> copyInitialMigrationData() {
        try {
            log.info("Copying initial migration data from source to target");
            Map<String, Object> result = migrationService.copyInitialData();
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("Error copying initial migration data", e);
            return ResponseEntity.status(500).body(Map.of(
                "success", false,
                "message", "Error: " + e.getMessage()
            ));
        }
    }

    /**
     * Get migration status
     */
    @GetMapping("/migration/status")
    public ResponseEntity<Map<String, Object>> getMigrationStatus() {
        try {
            Map<String, Object> status = migrationService.getMigrationStatus();
            return ResponseEntity.ok(status);
        } catch (Exception e) {
            log.error("Error getting migration status", e);
            return ResponseEntity.status(500).body(Map.of(
                "error", "Error: " + e.getMessage()
            ));
        }
    }

    /**
     * Get source database records
     */
    @GetMapping("/migration/source/records")
    public ResponseEntity<List<Map<String, Object>>> getMigrationSourceRecords() {
        try {
            List<Map<String, Object>> records = migrationService.getSourceRecords();
            return ResponseEntity.ok(records);
        } catch (Exception e) {
            log.error("Error getting migration source records", e);
            return ResponseEntity.status(500).body(List.of());
        }
    }

    /**
     * Get target database records
     */
    @GetMapping("/migration/target/records")
    public ResponseEntity<List<Map<String, Object>>> getMigrationTargetRecords() {
        try {
            List<Map<String, Object>> records = migrationService.getTargetRecords();
            return ResponseEntity.ok(records);
        } catch (Exception e) {
            log.error("Error getting migration target records", e);
            return ResponseEntity.status(500).body(List.of());
        }
    }

    /**
     * Verify sync between source and target
     */
    @GetMapping("/migration/verify")
    public ResponseEntity<Map<String, Object>> verifyMigrationSync() {
        try {
            Map<String, Object> result = migrationService.verifySync();
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("Error verifying migration sync", e);
            return ResponseEntity.status(500).body(Map.of(
                "success", false,
                "message", "Error: " + e.getMessage()
            ));
        }
    }
}

