package org.apache.shardingsphere.example.proxy.cdc.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * CDC Statistics Model
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CDCStatistics {
    private long totalEvents;
    private long insertCount;
    private long updateCount;
    private long deleteCount;
    private double eventsPerSecond;
    private long uptime;
}

