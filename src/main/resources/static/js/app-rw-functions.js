// Read-Write Splitting Functions

async function enableReplication() {
    try {
        const response = await fetch(`${API_BASE}/replication/enable`, {
            method: 'POST'
        });
        const result = await response.json();
        
        if (result.success) {
            alert('✅ Replication enabled successfully!');
            pollReplicationStatus();
            refreshReadWriteData();
        } else {
            alert('❌ Error: ' + result.message);
        }
    } catch (error) {
        console.error('Error enabling replication:', error);
        alert('❌ Failed to enable replication');
    }
}

async function disableReplication() {
    try {
        const response = await fetch(`${API_BASE}/replication/disable`, {
            method: 'POST'
        });
        const result = await response.json();
        
        if (result.success) {
            alert('✅ Replication disabled successfully!');
            pollReplicationStatus();
            refreshReadWriteData();
        } else {
            alert('❌ Error: ' + result.message);
        }
    } catch (error) {
        console.error('Error disabling replication:', error);
        alert('❌ Failed to disable replication');
    }
}

async function syncReplication() {
    try {
        const response = await fetch(`${API_BASE}/replication/sync`, {
            method: 'POST'
        });
        const result = await response.json();
        
        if (result.success) {
            alert('✅ Manual sync completed!');
            pollReplicationStatus();
            refreshReadWriteData();
        } else {
            alert('❌ Error: ' + result.message);
        }
    } catch (error) {
        console.error('Error syncing replication:', error);
        alert('❌ Failed to sync');
    }
}

async function switchDatabases() {
    if (!confirm('Are you sure you want to switch read and write database roles?\nThis will swap their assignments.')) {
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE}/replication/switch`, {
            method: 'POST'
        });
        const result = await response.json();
        
        if (result.success) {
            alert('✅ Database roles switched successfully!\nWrite DB ↔ Read DB');
            pollReplicationStatus();
            refreshReadWriteData();
        } else {
            alert('❌ Error: ' + result.message);
        }
    } catch (error) {
        console.error('Error switching databases:', error);
        alert('❌ Failed to switch databases');
    }
}

async function pollReplicationStatus() {
    try {
        const response = await fetch(`${API_BASE}/replication/status`);
        const status = await response.json();
        
        // Update dashboard status
        const dashboardStatus = document.getElementById('replicationStatus');
        if (dashboardStatus) {
            if (status.enabled) {
                dashboardStatus.textContent = 'Enabled';
                dashboardStatus.className = 'status-badge connected';
            } else {
                dashboardStatus.textContent = 'Disabled';
                dashboardStatus.className = 'status-badge disconnected';
            }
        }
        
        // Update dashboard counts
        const writeCountEl = document.getElementById('writeDbRecordCount');
        const readCountEl = document.getElementById('readDbRecordCount');
        if (writeCountEl) writeCountEl.textContent = status.writeRecordCount || 0;
        if (readCountEl) readCountEl.textContent = status.readRecordCount || 0;
        
        // Update Read-Write tab status
        const rwStatus = document.getElementById('rwReplicationStatus');
        const rwWriteCount = document.getElementById('rwWriteCount');
        const rwReadCount = document.getElementById('rwReadCount');
        const rwSyncStatus = document.getElementById('rwSyncStatus');
        
        if (rwStatus) {
            rwStatus.textContent = status.enabled ? 'Enabled ✅' : 'Disabled ❌';
            rwStatus.style.color = status.enabled ? '#10b981' : '#ef4444';
        }
        if (rwWriteCount) rwWriteCount.textContent = status.writeRecordCount || 0;
        if (rwReadCount) rwReadCount.textContent = status.readRecordCount || 0;
        if (rwSyncStatus) {
            if (status.inSync) {
                rwSyncStatus.textContent = 'In Sync ✅';
                rwSyncStatus.style.color = '#10b981';
            } else {
                rwSyncStatus.textContent = 'Out of Sync ⚠️';
                rwSyncStatus.style.color = '#f59e0b';
            }
        }
        
    } catch (error) {
        console.error('Error polling replication status:', error);
    }
}

async function refreshReadWriteData() {
    try {
        // Fetch write database data
        const writeResponse = await fetch(`${API_BASE}/writeData`);
        const writeResult = await writeResponse.json();
        
        if (writeResult.success) {
            renderWriteDbTable(writeResult.data || []);
            const writeCountEl = document.getElementById('writeDbCount');
            if (writeCountEl) writeCountEl.textContent = writeResult.data.length;
        }
        
        // Fetch read database data
        const readResponse = await fetch(`${API_BASE}/readData`);
        const readResult = await readResponse.json();
        
        if (readResult.success) {
            renderReadDbTable(readResult.data || []);
            const readCountEl = document.getElementById('readDbCount');
            if (readCountEl) readCountEl.textContent = readResult.data.length;
        }
        
    } catch (error) {
        console.error('Error refreshing read-write data:', error);
    }
}

function renderWriteDbTable(records) {
    const tableBody = document.getElementById('writeDbTable');
    if (!tableBody) return;
    
    tableBody.innerHTML = '';
    
    if (records.length === 0) {
        tableBody.innerHTML = '<tr><td colspan="3" style="text-align: center; color: #666;">No records in write database.</td></tr>';
        return;
    }
    
    records.forEach(record => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${record.order_id}</td>
            <td>${record.user_id}</td>
            <td>${record.status}</td>
        `;
        tableBody.appendChild(row);
    });
}

function renderReadDbTable(records) {
    const tableBody = document.getElementById('readDbTable');
    if (!tableBody) return;
    
    tableBody.innerHTML = '';
    
    if (records.length === 0) {
        tableBody.innerHTML = '<tr><td colspan="3" style="text-align: center; color: #666;">No records in read database.</td></tr>';
        return;
    }
    
    records.forEach(record => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${record.order_id}</td>
            <td>${record.user_id}</td>
            <td>${record.status}</td>
        `;
        tableBody.appendChild(row);
    });
}

