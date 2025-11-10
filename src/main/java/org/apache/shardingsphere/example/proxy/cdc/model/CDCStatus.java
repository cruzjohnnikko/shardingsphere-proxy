package org.apache.shardingsphere.example.proxy.cdc.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * CDC Status Model
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CDCStatus {
    private boolean connected;
    private boolean streaming;
    private String message;
}

