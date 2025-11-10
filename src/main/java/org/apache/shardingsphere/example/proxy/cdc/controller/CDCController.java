package org.apache.shardingsphere.example.proxy.cdc.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.example.proxy.cdc.model.CDCStatistics;
import org.apache.shardingsphere.example.proxy.cdc.model.CDCStatus;
import org.apache.shardingsphere.example.proxy.cdc.service.CDCClientService;
import org.apache.shardingsphere.example.proxy.cdc.service.DataGeneratorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.annotation.PostConstruct;
import java.util.Arrays;
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
            @RequestParam(defaultValue = "10") int count) {
        try {
            dataGeneratorService.generateData(count);
            return ResponseEntity.ok(Map.of("message", count + " records generated"));
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
    public ResponseEntity<Map<String, Long>> getSourceRecordCount() {
        try {
            long count = dataGeneratorService.getSourceRecordCount();
            return ResponseEntity.ok(Map.of("count", count));
        } catch (Exception e) {
            log.error("Error getting source record count", e);
            return ResponseEntity.status(500).build();
        }
    }

    @GetMapping("/targetRecordCount")
    public ResponseEntity<Map<String, Long>> getTargetRecordCount() {
        try {
            long count = dataGeneratorService.getTargetRecordCount();
            return ResponseEntity.ok(Map.of("count", count));
        } catch (Exception e) {
            log.error("Error getting target record count", e);
            return ResponseEntity.status(500).build();
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
        return ResponseEntity.ok(Map.of("running", false, "interval", 1000));
    }

    @PostMapping("/generator/start")
    public ResponseEntity<Map<String, String>> startGenerator(
            @RequestParam(defaultValue = "1000") int interval,
            @RequestParam(defaultValue = "insert") String operation) {
        return ResponseEntity.ok(Map.of("message", "Generator not implemented - use Generate Data button instead"));
    }

    @PostMapping("/generator/stop")
    public ResponseEntity<Map<String, String>> stopGenerator() {
        return ResponseEntity.ok(Map.of("message", "Generator stopped"));
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
}

