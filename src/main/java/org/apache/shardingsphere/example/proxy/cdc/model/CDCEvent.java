package org.apache.shardingsphere.example.proxy.cdc.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * CDC Event Model
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CDCEvent {
    private String eventType;
    private String tableName;
    private String database;
    private Map<String, Object> data;
    private Map<String, Object> beforeData;
    private Map<String, Object> afterData;
    private long timestamp;
    
    // Legacy getter for compatibility
    public Map<String, Object> getAfterData() {
        return afterData != null ? afterData : data;
    }
    
    public Map<String, Object> getBeforeData() {
        return beforeData;
    }
}
