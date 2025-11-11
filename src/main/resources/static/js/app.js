// API Configuration
const API_BASE = 'http://localhost:8080/api/cdc';
const WS_URL = 'ws://localhost:8080/ws/cdc';

// WebSocket connection
let ws = null;
let reconnectInterval = null;

// Replication disabled for now
// let replicationEnabled = false;
// let replicationInterval = null;

// DOM Elements
const cdcStatusEl = document.getElementById('cdcStatus');
const generatorStatusEl = document.getElementById('generatorStatus');
const wsStatusEl = document.getElementById('wsStatus');
const eventsContainerEl = document.getElementById('eventsContainer');
const messagesContainerEl = document.getElementById('messagesContainer');
const eventCountEl = document.getElementById('eventCount');
const recordCountEl = document.getElementById('recordCount');
const clearDataCheckbox = document.getElementById('clearDataCheckbox');
const clearTargetCheckbox = document.getElementById('clearTargetCheckbox');

// Statistics Elements
const totalEventsEl = document.getElementById('totalEvents');
const insertCountEl = document.getElementById('insertCount');
const updateCountEl = document.getElementById('updateCount');
const deleteCountEl = document.getElementById('deleteCount');
const eventsPerSecondEl = document.getElementById('eventsPerSecond');
const uptimeEl = document.getElementById('uptime');

// Button Elements
const connectBtn = document.getElementById('connectBtn');
const startStreamingBtn = document.getElementById('startStreamingBtn');
const stopStreamingBtn = document.getElementById('stopStreamingBtn');
const stopBtn = document.getElementById('stopBtn');
const resetBtn = document.getElementById('resetBtn');
const startGeneratorBtn = document.getElementById('startGeneratorBtn');
const stopGeneratorBtn = document.getElementById('stopGeneratorBtn');
const manualInsertBtn = document.getElementById('manualInsertBtn');

// Event counter
let eventCount = 0;

// Comparison refresh interval
let comparisonRefreshInterval = null;

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    setupEventListeners();
    setupTabSwitching();
    connectWebSocket();
    pollStatus();
    updateRecordCount(); // Initial record count
    updateDashboardStats(); // Initial dashboard stats
    setInterval(pollStatus, 2000);
    setInterval(updateRecordCount, 3000); // Update record count every 3 seconds
    setInterval(updateDashboardStats, 3000); // Update dashboard stats every 3 seconds
    // Don't call refreshReadWriteData here - it will be called when user switches to that tab
});

// Setup Event Listeners
function setupEventListeners() {
    if (connectBtn) connectBtn.addEventListener('click', connect);
    if (startStreamingBtn) startStreamingBtn.addEventListener('click', startStreaming);
    if (stopStreamingBtn) stopStreamingBtn.addEventListener('click', stop);
    if (stopBtn) stopBtn.addEventListener('click', stop);
    if (resetBtn) resetBtn.addEventListener('click', resetStatistics);
    if (startGeneratorBtn) startGeneratorBtn.addEventListener('click', startGenerator);
    if (stopGeneratorBtn) stopGeneratorBtn.addEventListener('click', stopGenerator);
    if (manualInsertBtn) manualInsertBtn.addEventListener('click', manualInsert);
    
    // Read-Write Splitting Event Listeners
    const enableReplicationBtn = document.getElementById('enableReplicationBtn');
    const disableReplicationBtn = document.getElementById('disableReplicationBtn');
    const syncReplicationBtn = document.getElementById('syncReplicationBtn');
    const switchDatabasesBtn = document.getElementById('switchDatabasesBtn');
    const rwEnableBtn = document.getElementById('rwEnableBtn');
    const rwDisableBtn = document.getElementById('rwDisableBtn');
    const rwSyncBtn = document.getElementById('rwSyncBtn');
    const rwSwitchBtn = document.getElementById('rwSwitchBtn');
    const rwRefreshBtn = document.getElementById('rwRefreshBtn');
    const rwInsertBtn = document.getElementById('rwInsertBtn');
    const rwSearchBtn = document.getElementById('rwSearchBtn');
    const rwClearSearchBtn = document.getElementById('rwClearSearchBtn');
    
    if (enableReplicationBtn) enableReplicationBtn.addEventListener('click', enableReplication);
    if (disableReplicationBtn) disableReplicationBtn.addEventListener('click', disableReplication);
    if (syncReplicationBtn) syncReplicationBtn.addEventListener('click', syncReplication);
    if (switchDatabasesBtn) switchDatabasesBtn.addEventListener('click', switchDatabases);
    if (rwEnableBtn) rwEnableBtn.addEventListener('click', enableReplication);
    if (rwDisableBtn) rwDisableBtn.addEventListener('click', disableReplication);
    if (rwSyncBtn) rwSyncBtn.addEventListener('click', syncReplication);
    if (rwSwitchBtn) rwSwitchBtn.addEventListener('click', switchDatabases);
    if (rwRefreshBtn) rwRefreshBtn.addEventListener('click', refreshReadWriteData);
    if (rwInsertBtn) rwInsertBtn.addEventListener('click', insertToWriteDb);
    if (rwSearchBtn) rwSearchBtn.addEventListener('click', searchReadDatabase);
    if (rwClearSearchBtn) rwClearSearchBtn.addEventListener('click', clearSearch);
    
    // Migration Event Listeners
    const migrationSetupBtn = document.getElementById('migrationSetupBtn');
    const migrationStartBtn = document.getElementById('migrationStartBtn');
    const migrationStopBtn = document.getElementById('migrationStopBtn');
    const migrationVerifyBtn = document.getElementById('migrationVerifyBtn');
    const migrationRefreshBtn = document.getElementById('migrationRefreshBtn');
    
    if (migrationSetupBtn) migrationSetupBtn.addEventListener('click', setupMigrationSource);
    if (migrationStartBtn) migrationStartBtn.addEventListener('click', startMigration);
    if (migrationStopBtn) migrationStopBtn.addEventListener('click', stopMigration);
    if (migrationVerifyBtn) migrationVerifyBtn.addEventListener('click', verifyMigration);
    if (migrationRefreshBtn) migrationRefreshBtn.addEventListener('click', refreshMigrationData);
    
    // Clear Database Button Event Listeners
    const clearShardingSphereBtn = document.getElementById('clearShardingSphereBtn');
    const clearMigrationSourceBtn = document.getElementById('clearMigrationSourceBtn');
    const clearTargetBtn = document.getElementById('clearTargetBtn');
    
    if (clearShardingSphereBtn) clearShardingSphereBtn.addEventListener('click', clearShardingSphereDb);
    if (clearMigrationSourceBtn) clearMigrationSourceBtn.addEventListener('click', clearMigrationSourceDb);
    if (clearTargetBtn) clearTargetBtn.addEventListener('click', clearTargetDb);
}

// WebSocket Functions
function connectWebSocket() {
    try {
        ws = new WebSocket(WS_URL);
        
        ws.onopen = () => {
            console.log('WebSocket connected');
            updateWebSocketStatus(true);
            addMessage('WebSocket connected', 'success');
            if (reconnectInterval) {
                clearInterval(reconnectInterval);
                reconnectInterval = null;
            }
        };
        
        ws.onmessage = (event) => {
            console.log('📥 Raw WebSocket message:', event.data);
            try {
                const message = JSON.parse(event.data);
                console.log('📦 Parsed message:', message);
                handleWebSocketMessage(message);
            } catch (error) {
                console.error('❌ Error parsing WebSocket message:', error);
            }
        };
        
        ws.onerror = (error) => {
            console.error('WebSocket error:', error);
            addMessage('WebSocket error', 'error');
        };
        
        ws.onclose = () => {
            console.log('WebSocket disconnected');
            updateWebSocketStatus(false);
            addMessage('WebSocket disconnected. Reconnecting...', 'error');
            
            if (!reconnectInterval) {
                reconnectInterval = setInterval(() => {
                    console.log('Attempting to reconnect WebSocket...');
                    connectWebSocket();
                }, 5000);
            }
        };
    } catch (error) {
        console.error('Failed to create WebSocket:', error);
        updateWebSocketStatus(false);
    }
}

function handleWebSocketMessage(message) {
    console.log('🔔 WebSocket message received:', message);
    console.log('   Message type:', message.type);
    
    switch (message.type) {
        case 'cdcEvent':
        case 'event':
            console.log('✅ CDC Event detected! Payload:', message.payload || message.data);
            const eventData = message.payload || message.data;
            if (eventData) {
                console.log('   Calling addCdcEventToTable with:', eventData);
                addCdcEventToTable(eventData);
                eventCount++;
                if (eventCountEl) eventCountEl.textContent = eventCount.toLocaleString();
                console.log('   Event added, total count:', eventCount);
            } else {
                console.error('❌ No event data in message!');
            }
            break;
        case 'cdcStatistics':
        case 'statistics':
            console.log('📊 Statistics update:', message.payload || message.data);
            updateStatistics(message.payload || message.data);
            break;
        case 'status':
            console.log('ℹ️ Status message:', message.message);
            addMessage(message.message, 'info');
            break;
        case 'error':
            console.error('❌ Error message:', message.message);
            addMessage(message.message, 'error');
            break;
        default:
            console.warn('⚠️ Unknown message type:', message.type);
    }
}

function updateWebSocketStatus(connected) {
    if (connected) {
        wsStatusEl.textContent = 'Connected';
        wsStatusEl.className = 'status-badge connected';
    } else {
        wsStatusEl.textContent = 'Disconnected';
        wsStatusEl.className = 'status-badge disconnected';
    }
}

// API Functions
async function connect() {
    try {
        if (connectBtn) connectBtn.disabled = true;
        
        showNotification('⏳ Connecting to ShardingSphere Proxy...', 'info');
        
        const response = await fetch(`${API_BASE}/connect`, { method: 'POST' });
        const result = await response.json();
        
        if (result.success) {
            addMessage(result.message, 'success');
            showNotification('✅ ' + result.message, 'success');
            
            if (startStreamingBtn) startStreamingBtn.disabled = false;
            if (stopBtn) stopBtn.disabled = true;
        } else {
            addMessage(result.message, 'error');
            showNotification('❌ ' + result.message, 'error');
            
            if (connectBtn) connectBtn.disabled = false;
            if (startStreamingBtn) startStreamingBtn.disabled = true;
            if (stopBtn) stopBtn.disabled = true;
        }
    } catch (error) {
        console.error('Failed to connect:', error);
        addMessage('Failed to connect: ' + error.message, 'error');
        showNotification('❌ Failed to connect: ' + error.message, 'error');
        
        if (connectBtn) connectBtn.disabled = false;
        if (startStreamingBtn) startStreamingBtn.disabled = true;
        if (stopBtn) stopBtn.disabled = true;
    }
}

async function startStreaming() {
    try {
        if (startStreamingBtn) startStreamingBtn.disabled = true;
        if (stopBtn) stopBtn.disabled = false;
        if (connectBtn) connectBtn.disabled = true;
        
        showNotification('⏳ Starting CDC streaming from ShardingSphere DB...', 'info');
        
        const response = await fetch(`${API_BASE}/startStreaming?database=sharding_db&tables=t_order`, { method: 'POST' });
        const result = await response.text();
        
        addMessage(result, 'success');
        showNotification('✅ ' + result, 'success');
    } catch (error) {
        console.error('Failed to start streaming:', error);
        addMessage('Failed to start streaming: ' + error.message, 'error');
        showNotification('❌ Failed to start streaming: ' + error.message, 'error');
        
        if (startStreamingBtn) startStreamingBtn.disabled = false;
        if (stopBtn) stopBtn.disabled = true;
        if (connectBtn) connectBtn.disabled = false;
    }
}

async function stop() {
    try {
        if (stopStreamingBtn) stopStreamingBtn.disabled = true;
        if (startStreamingBtn) startStreamingBtn.disabled = false;
        
        showNotification('⏳ Stopping CDC streaming...', 'info');
        
        const response = await fetch(`${API_BASE}/stopStreaming`, { method: 'POST' });
        const result = await response.text();
        
        addMessage(result, 'success');
        showNotification('✅ ' + result, 'success');
        
        // Re-enable the stop button after successful stop
        if (stopStreamingBtn) stopStreamingBtn.disabled = true;
    } catch (error) {
        console.error('Failed to stop:', error);
        addMessage('Failed to stop: ' + error.message, 'error');
        showNotification('❌ Failed to stop: ' + error.message, 'error');
        
        if (stopStreamingBtn) stopStreamingBtn.disabled = false;
        if (startStreamingBtn) startStreamingBtn.disabled = true;
    }
}

async function resetStatistics() {
    try {
        const clearData = clearDataCheckbox ? clearDataCheckbox.checked : false;
        const clearTarget = clearTargetCheckbox ? clearTargetCheckbox.checked : false;
        
        // Confirm if clearing data
        if (clearData || clearTarget) {
            let message = '⚠️ Warning: This will delete data from:\n';
            if (clearData) message += `- Source database (${recordCountEl ? recordCountEl.textContent : 'N/A'} records)\n`;
            if (clearTarget) message += '- Target database\n';
            message += '\nAre you sure you want to continue?';
            
            if (!confirm(message)) {
                return;
            }
        }
        
        const response = await fetch(`${API_BASE}/reset?clearData=${clearData}&clearTarget=${clearTarget}`, { method: 'POST' });
        const result = await response.json();
        
        if (result.success) {
            addMessage(result.message, 'success');
            eventCount = 0;
            updateEventCount();
            // Clear events display
            eventsContainerEl.innerHTML = '<div class="no-events">Statistics reset. Waiting for new events...</div>';
            
            // Update record count
            await updateRecordCount();
            
            // Uncheck the checkboxes after reset
            clearDataCheckbox.checked = false;
            clearTargetCheckbox.checked = false;
        } else {
            addMessage(result.message, 'error');
        }
    } catch (error) {
        addMessage('Failed to reset statistics: ' + error.message, 'error');
    }
}

// Update database record count
async function updateRecordCount() {
    try {
        const response = await fetch(`${API_BASE}/recordCount`);
        const result = await response.json();
        
        if (result.success) {
            if (recordCountEl) recordCountEl.textContent = result.count.toLocaleString();
        } else {
            if (recordCountEl) recordCountEl.textContent = 'Error';
        }
    } catch (error) {
        console.error('Failed to fetch record count:', error);
        if (recordCountEl) recordCountEl.textContent = '?';
    }
}

// Update dashboard statistics (for main dashboard view)
async function updateDashboardStats() {
    try {
        console.log('Updating dashboard stats...');
        // Fetch counts
        const [sourceCountRes, targetCountRes, statsRes] = await Promise.all([
            fetch(`${API_BASE}/sourceRecordCount`),
            fetch(`${API_BASE}/targetRecordCount`),
            fetch(`${API_BASE}/statistics`)
        ]);
        
        const sourceCount = await sourceCountRes.json();
        const targetCount = await targetCountRes.json();
        const stats = await statsRes.json();
        
        console.log('Source count:', sourceCount);
        console.log('Target count:', targetCount);
        console.log('Stats:', stats);
        
        // Update Source Records
        const sourceEl = document.getElementById('sourceRecordCount');
        if (sourceEl && sourceCount && sourceCount.count !== undefined) {
            sourceEl.textContent = sourceCount.count.toLocaleString();
        }
        
        // Update Target Records
        const targetEl = document.getElementById('targetRecordCount');
        if (targetEl && targetCount && targetCount.count !== undefined) {
            targetEl.textContent = targetCount.count.toLocaleString();
        }
        
        // Update Unsynced Records
        const unsynced = Math.max(0, (sourceCount.count || 0) - (targetCount.count || 0));
        const unsyncedEl = document.getElementById('unsyncedRecordCount');
        if (unsyncedEl) {
            unsyncedEl.textContent = unsynced.toLocaleString();
        }
        
        // Update Total Events Captured
        const eventsEl = document.getElementById('totalEventsCaptured');
        if (eventsEl && stats && stats.totalEvents !== undefined) {
            eventsEl.textContent = stats.totalEvents.toLocaleString();
        }
        
        console.log('Dashboard stats updated successfully');
    } catch (error) {
        console.error('Failed to update dashboard stats:', error);
        console.error('Error stack:', error.stack);
    }
}

async function startGenerator() {
    try {
        const interval = document.getElementById('interval').value;
        const operationType = document.getElementById('operationType').value;
        
        startGeneratorBtn.disabled = true;
        const response = await fetch(`${API_BASE}/generator/start?interval=${interval}&operation=${operationType}`, { 
            method: 'POST' 
        });
        const result = await response.json();
        
        if (result.success) {
            addMessage(result.message, 'success');
            stopGeneratorBtn.disabled = false;
        } else {
            addMessage(result.message, 'error');
            startGeneratorBtn.disabled = false;
        }
    } catch (error) {
        addMessage('Failed to start generator: ' + error.message, 'error');
        startGeneratorBtn.disabled = false;
    }
}

async function stopGenerator() {
    try {
        stopGeneratorBtn.disabled = true;
        const response = await fetch(`${API_BASE}/generator/stop`, { method: 'POST' });
        const result = await response.json();
        
        if (result.success) {
            addMessage(result.message, 'success');
            startGeneratorBtn.disabled = false;
        } else {
            addMessage(result.message, 'error');
            stopGeneratorBtn.disabled = false;
        }
    } catch (error) {
        addMessage('Failed to stop generator: ' + error.message, 'error');
        stopGeneratorBtn.disabled = false;
    }
}

async function manualInsert() {
    try {
        const userId = document.getElementById('userId').value;
        const status = document.getElementById('orderStatus').value;
        
        const response = await fetch(`${API_BASE}/generator/insert?userId=${userId}&status=${status}`, { 
            method: 'POST' 
        });
        const result = await response.json();
        
        if (result.success) {
            addMessage('Order inserted successfully', 'success');
        } else {
            addMessage(result.message, 'error');
        }
    } catch (error) {
        addMessage('Failed to insert order: ' + error.message, 'error');
    }
}

async function pollStatus() {
    try {
        const [statusResponse, generatorResponse] = await Promise.all([
            fetch(`${API_BASE}/status`),
            fetch(`${API_BASE}/generator/status`)
        ]);
        
        const status = await statusResponse.json();
        const generatorStatus = await generatorResponse.json();
        
        updateCDCStatus(status);
        updateGeneratorStatus(generatorStatus.running);
        
        if (!status.streaming) {
            // Also fetch statistics even when not streaming
            const statsResponse = await fetch(`${API_BASE}/statistics`);
            const stats = await statsResponse.json();
            updateStatistics(stats);
        }
    } catch (error) {
        console.error('Failed to poll status:', error);
    }
}

function updateCDCStatus(status) {
    if (!status) {
        console.warn('updateCDCStatus called with undefined status');
        return;
    }
    
    const cdcStreamingStatusText = document.getElementById('cdcStreamingStatusText');
    const stopStreamingBtn = document.getElementById('stopStreamingBtn');
    
    if (status.streaming) {
        if (cdcStatusEl) {
        cdcStatusEl.textContent = 'Streaming';
        cdcStatusEl.className = 'status-badge streaming';
        }
        // Update status text in replication tab
        if (cdcStreamingStatusText) {
            cdcStreamingStatusText.textContent = '✅ CDC is actively streaming changes';
        }
        // Update button states (with null checks for Dashboard controls that were removed)
        if (startStreamingBtn) startStreamingBtn.disabled = true;
        if (stopStreamingBtn) stopStreamingBtn.disabled = false;
        if (stopBtn) stopBtn.disabled = false;
        if (connectBtn) connectBtn.disabled = true;
    } else if (status.connected) {
        if (cdcStatusEl) {
        cdcStatusEl.textContent = 'Connected';
        cdcStatusEl.className = 'status-badge connected';
        }
        // Update status text in replication tab
        if (cdcStreamingStatusText) {
            cdcStreamingStatusText.textContent = '⏸️ Not streaming (Connected)';
        }
        // Update button states
        if (startStreamingBtn) startStreamingBtn.disabled = false;
        if (stopStreamingBtn) stopStreamingBtn.disabled = true;
        if (stopBtn) stopBtn.disabled = true;
        if (connectBtn) connectBtn.disabled = true;
    } else {
        if (cdcStatusEl) {
        cdcStatusEl.textContent = 'Disconnected';
        cdcStatusEl.className = 'status-badge disconnected';
        }
        // Update status text in replication tab
        if (cdcStreamingStatusText) {
            cdcStreamingStatusText.textContent = '⏸️ Not streaming';
        }
        // Update button states
        if (startStreamingBtn) startStreamingBtn.disabled = true;
        if (stopStreamingBtn) stopStreamingBtn.disabled = true;
        if (stopBtn) stopBtn.disabled = true;
        if (connectBtn) connectBtn.disabled = false;
    }
}

function updateGeneratorStatus(running) {
    if (!generatorStatusEl) return;
    
    if (running) {
        generatorStatusEl.textContent = 'Running';
        generatorStatusEl.className = 'status-badge running';
    } else {
        generatorStatusEl.textContent = 'Stopped';
        generatorStatusEl.className = 'status-badge stopped';
    }
}

function updateStatistics(stats) {
    if (totalEventsEl) totalEventsEl.textContent = formatNumber(stats.totalEvents);
    if (insertCountEl) insertCountEl.textContent = formatNumber(stats.insertCount);
    if (updateCountEl) updateCountEl.textContent = formatNumber(stats.updateCount);
    if (deleteCountEl) deleteCountEl.textContent = formatNumber(stats.deleteCount);
    if (eventsPerSecondEl) eventsPerSecondEl.textContent = stats.eventsPerSecond.toFixed(2);
    if (uptimeEl) uptimeEl.textContent = formatUptime(stats.uptime);
}

function addEvent(event) {
    // Remove "no events" message if present
    const noEvents = eventsContainerEl.querySelector('.no-events');
    if (noEvents) {
        noEvents.remove();
    }
    
    eventCount++;
    updateEventCount();
    
    const eventItem = document.createElement('div');
    eventItem.className = `event-item ${event.eventType.toLowerCase()}`;
    
    const eventHtml = `
        <div class="event-header">
            <span class="event-type ${event.eventType.toLowerCase()}">${event.eventType}</span>
            <span class="event-time">${formatTime(event.timestamp)}</span>
        </div>
        <div class="event-details">
            <span class="event-table">${event.schemaName}.${event.tableName}</span>
            ${event.transactionId ? ` | Transaction: ${event.transactionId}` : ''}
        </div>
    `;
    
    eventItem.innerHTML = eventHtml;
    
    // Insert at the beginning
    eventsContainerEl.insertBefore(eventItem, eventsContainerEl.firstChild);
    
    // Keep only last 50 events
    while (eventsContainerEl.children.length > 50) {
        eventsContainerEl.removeChild(eventsContainerEl.lastChild);
    }
}

function addCdcEventToTable(event) {
    console.log('Adding CDC event to table:', event);
    
    // Try both possible table IDs
    let tableBody = document.getElementById('cdcEventsTable');
    if (!tableBody) {
        tableBody = document.getElementById('eventsTableBody');
    }
    
    if (!tableBody) {
        console.error('CDC Events table not found (tried cdcEventsTable and eventsTableBody)');
        return;
    }
    
    console.log('Table found:', tableBody.id, 'Current rows:', tableBody.rows.length);
    
    // Remove placeholder if exists
    if (tableBody.rows.length === 1 && tableBody.rows[0].cells.length === 1) {
        tableBody.innerHTML = '';
    }
    
    // Handle different field names
    const beforeData = event.beforeData || event.before || {};
    const afterData = event.afterData || event.after || event.data || {};
    
    // Check if table expects detailed columns (Order ID, User ID, Status) or generic columns
    const firstRow = tableBody.parentElement.querySelector('thead tr');
    const headers = firstRow ? Array.from(firstRow.cells).map(c => c.textContent.trim()) : [];
    
    console.log('Table headers:', headers);
    
    const row = tableBody.insertRow(0);
    
    if (headers.includes('Order ID') || headers.includes('User ID')) {
        // Detailed table format: Type | Order ID | User ID | Status (Before/After) | Time
        row.insertCell(0).textContent = event.eventType || 'UNKNOWN';
        row.insertCell(1).textContent = afterData.order_id || beforeData.order_id || '-';
        row.insertCell(2).textContent = afterData.user_id || beforeData.user_id || '-';
        
        // Status column showing before/after
        let statusText = '';
        if (event.eventType === 'INSERT') {
            statusText = afterData.status || '-';
        } else if (event.eventType === 'UPDATE') {
            statusText = `${beforeData.status || '?'} → ${afterData.status || '?'}`;
        } else if (event.eventType === 'DELETE') {
            statusText = beforeData.status || '-';
        }
        row.insertCell(3).textContent = statusText;
        row.insertCell(4).textContent = new Date().toLocaleTimeString();
    } else {
        // Generic table format: Event Type | Database | Table Name | Before Data | After Data
        row.insertCell(0).textContent = event.eventType || 'UNKNOWN';
        row.insertCell(1).textContent = event.database || '-';
        row.insertCell(2).textContent = event.tableName || '-';
        row.insertCell(3).textContent = Object.keys(beforeData).length > 0 ? JSON.stringify(beforeData) : '-';
        row.insertCell(4).textContent = Object.keys(afterData).length > 0 ? JSON.stringify(afterData) : '-';
    }
    
    // Keep only last 50 events
    if (tableBody.rows.length > 50) {
        tableBody.deleteRow(tableBody.rows.length - 1);
    }
    
    console.log('Event added successfully, total rows now:', tableBody.rows.length);
}

function addMessage(message, type) {
    if (!messagesContainerEl) {
        console.log(`[${type}] ${message}`);
        return;
    }
    
    const messageEl = document.createElement('div');
    messageEl.className = `message ${type}`;
    messageEl.innerHTML = `<span class="message-time">${formatTime(Date.now())}</span>${message}`;
    
    messagesContainerEl.insertBefore(messageEl, messagesContainerEl.firstChild);
    
    // Keep only last 20 messages
    while (messagesContainerEl.children.length > 20) {
        messagesContainerEl.removeChild(messagesContainerEl.lastChild);
    }
}

function updateEventCount() {
    eventCountEl.textContent = `(${eventCount})`;
}

// Utility Functions
function formatTime(timestamp) {
    const date = new Date(timestamp);
    return date.toLocaleTimeString();
}

function formatNumber(num) {
    return num.toLocaleString();
}

function formatUptime(seconds) {
    if (seconds < 60) {
        return `${seconds}s`;
    } else if (seconds < 3600) {
        return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
    } else {
        const hours = Math.floor(seconds / 3600);
        const minutes = Math.floor((seconds % 3600) / 60);
        return `${hours}h ${minutes}m`;
    }
}


// Read-Write Splitting refresh interval
let readWriteRefreshInterval = null;

// Tab Switching
function setupTabSwitching() {
    const tabButtons = document.querySelectorAll('.tab-btn');
    const tabContents = document.querySelectorAll('.tab-content');
    
    tabButtons.forEach(button => {
        button.addEventListener('click', () => {
            const targetTab = button.dataset.tab;
            
            // Remove active class from all buttons and content
            tabButtons.forEach(btn => btn.classList.remove('active'));
            tabContents.forEach(content => content.classList.remove('active'));
            
            // Add active class to clicked button
            button.classList.add('active');
            
            // Show corresponding tab content
            const targetContent = document.getElementById(targetTab + 'Tab');
            if (targetContent) {
                targetContent.classList.add('active');
            }
            
            // Clear all refresh intervals first
                if (comparisonRefreshInterval) {
                    clearInterval(comparisonRefreshInterval);
                    comparisonRefreshInterval = null;
                }
            if (readWriteRefreshInterval) {
                clearInterval(readWriteRefreshInterval);
                readWriteRefreshInterval = null;
            }
            if (migrationRefreshInterval) {
                clearInterval(migrationRefreshInterval);
                migrationRefreshInterval = null;
            }
            
            // Start appropriate refresh based on active tab
            if (targetTab === 'replication') {
                // Merged CDC Replication & Migration Tab - load all three databases
                fetchDataComparison(); // Load ShardingSphere DB → Target DB
                refreshMigrationData(); // Load Source DB (migration)
                comparisonRefreshInterval = setInterval(() => {
                    fetchDataComparison();
                    refreshMigrationData();
                }, 3000);
            } else if (targetTab === 'readwrite') {
                refreshReadWriteData(); // Load immediately
                pollReplicationStatus(); // Update status immediately
                readWriteRefreshInterval = setInterval(() => {
                    refreshReadWriteData();
                    pollReplicationStatus();
                }, 3000);
            }
        });
    });
}

// Fetch and display data comparison
async function fetchDataComparison() {
    try {
        // Fetch Source Data
        const sourceResponse = await fetch(`${API_BASE}/sourceData?limit=50`);
        const sourceResult = await sourceResponse.json();
        let sourceRecords = [];
        
        if (sourceResult && sourceResult.success && Array.isArray(sourceResult.data)) {
            sourceRecords = sourceResult.data;
            document.getElementById('sourceRecordCount').textContent = sourceRecords.length.toLocaleString();
            renderSourceDataTable(sourceRecords);
        } else {
            document.getElementById('sourceRecordCount').textContent = 'Error';
            document.getElementById('sourceDataTable').innerHTML = 
                `<tr><td colspan="3" style="color: #ef4444;">${(sourceResult && sourceResult.message) || 'Failed to load'}</td></tr>`;
        }

        // Fetch Target Data
        const targetResponse = await fetch(`${API_BASE}/targetData?limit=50`);
        const targetResult = await targetResponse.json();
        let targetRecords = [];
        
        if (targetResult && targetResult.success && Array.isArray(targetResult.data)) {
            targetRecords = targetResult.data;
            document.getElementById('targetRecordCount').textContent = targetRecords.length.toLocaleString();
            renderTargetDataTable(targetRecords);
        } else {
            document.getElementById('targetRecordCount').textContent = 'Error';
            document.getElementById('targetDataTable').innerHTML = 
                `<tr><td colspan="3" style="color: #ef4444;">${(targetResult && targetResult.message) || 'Failed to load'}</td></tr>`;
        }

        // Fetch CDC Statistics and Events
        const statsResponse = await fetch(`${API_BASE}/statistics`);
        const stats = await statsResponse.json();
        const cdcEventsResponse = await fetch(`${API_BASE}/events?limit=50`);
        const cdcEvents = await cdcEventsResponse.json();

        // Display total events captured (all CDC events regardless of type)
        document.getElementById('totalEventsCaptured').textContent = stats.totalEvents ? stats.totalEvents.toLocaleString() : '0';
        
        // Don't call renderCdcEventsTable here - it clears the table!
        // The table is populated in real-time via WebSocket (addCdcEventToTable)
        // Only render if table is completely empty
        const tableBody = document.getElementById('cdcEventsTable');
        if (tableBody.rows.length === 0 || (tableBody.rows.length === 1 && tableBody.rows[0].cells.length === 1 && tableBody.rows[0].cells[0].textContent.includes('No CDC'))) {
            console.log('CDC Events table is empty, rendering placeholder');
            renderCdcEventsTable(cdcEvents || []);
        } else {
            console.log('CDC Events table has', tableBody.rows.length, 'rows, preserving them');
        }

        // Calculate Unsynced Records (only positive values make sense)
        // Unsynced = source records that haven't been synced to target yet
        // If target has more records than source (shouldn't happen normally), unsynced = 0
        const sourceCount = Array.isArray(sourceRecords) ? sourceRecords.length : 0;
        const targetCount = Array.isArray(targetRecords) ? targetRecords.length : 0;
        let unsyncedCount = Math.max(0, sourceCount - targetCount);
        
        const unsyncedRecordCountEl = document.getElementById('unsyncedRecordCount');
        if (unsyncedRecordCountEl) unsyncedRecordCountEl.textContent = unsyncedCount.toLocaleString();
        
        // Update badges for merged tab
        const sourceBadgeEl = document.getElementById('sourceBadge');
        if (sourceBadgeEl) sourceBadgeEl.textContent = sourceCount;
        
        const targetBadgeEl = document.getElementById('targetBadge');
        if (targetBadgeEl) targetBadgeEl.textContent = targetCount;

        // Update CDC Job Status
        const cdcStatusResponse = await fetch(`${API_BASE}/status`);
        const cdcStatus = await cdcStatusResponse.json();
        const jobStatusEl = document.getElementById('cdcJobStatus');
        
        if (cdcStatus.streaming) {
            jobStatusEl.textContent = 'Streaming';
            jobStatusEl.className = 'status-indicator running';
        } else if (cdcStatus.connected) {
            jobStatusEl.textContent = 'Connected';
            jobStatusEl.className = 'status-indicator connected';
        } else {
            jobStatusEl.textContent = 'Not Running';
            jobStatusEl.className = 'status-indicator not-running';
        }

    } catch (error) {
        console.error('Error fetching data comparison:', error);
        // Set defaults instead of Error (with null checks)
        const safeSetText = (id, defaultValue) => {
            const el = document.getElementById(id);
            if (el) el.textContent = defaultValue;
        };
        safeSetText('sourceRecordCount', '0');
        safeSetText('targetRecordCount', '0');
        safeSetText('unsyncedRecordCount', '0');
        safeSetText('totalEventsCaptured', '0');
        safeSetText('cdcJobStatus', 'Not Running');
        safeSetText('sourceBadge', '0');
        safeSetText('targetBadge', '0');
        const statusEl = document.getElementById('cdcJobStatus');
        if (statusEl) statusEl.className = 'status-indicator not-running';
    }
}

function renderSourceDataTable(records) {
    const tableBody = document.getElementById('sourceDataTable');
    tableBody.innerHTML = '';
    
    if (records.length === 0) {
        tableBody.innerHTML = '<tr><td colspan="3" style="text-align: center; color: #666;">No records in source database.</td></tr>';
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

function renderTargetDataTable(records) {
    const tableBody = document.getElementById('targetDataTable');
    tableBody.innerHTML = '';
    
    if (records.length === 0) {
        tableBody.innerHTML = '<tr><td colspan="3" style="text-align: center; color: #666;">No records in target database.</td></tr>';
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

function renderWriteDbDataTable(records) {
    const tableBody = document.getElementById('writeDbDataTable');
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

function renderReadDbDataTable(records) {
    const tableBody = document.getElementById('readDbDataTable');
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

function renderCdcEventsTable(events) {
    console.log('renderCdcEventsTable called with', events.length, 'events');
    const tableBody = document.getElementById('cdcEventsTable');
    
    // DON'T clear existing rows - this function is called on tab switch
    // Only show "no events" message if table is truly empty AND no events to render
    if (tableBody.rows.length === 0 || (tableBody.rows.length === 1 && tableBody.rows[0].cells.length === 1)) {
        if (events.length === 0) {
            tableBody.innerHTML = '<tr><td colspan="5" style="text-align: center; color: #666;">No CDC events captured yet.</td></tr>';
            return;
        }
        // Table was empty, now populate it
        tableBody.innerHTML = '';
    } else {
        // Table already has events from WebSocket, don't touch it!
        console.log('Table already has', tableBody.rows.length, 'rows - preserving them');
        return;
    }
    
    events.forEach(event => {
        let statusDisplay = '';
        if (event.eventType === 'INSERT') {
            statusDisplay = event.afterData.status || 'N/A';
        } else if (event.eventType === 'UPDATE') {
            statusDisplay = `${event.beforeData.status || 'N/A'} → ${event.afterData.status || 'N/A'}`;
        } else if (event.eventType === 'DELETE') {
            statusDisplay = event.beforeData.status || 'N/A';
        } else {
            statusDisplay = 'N/A';
        }

        const row = document.createElement('tr');
        row.innerHTML = `
            <td class="event-type-${event.eventType.toLowerCase()}">${event.eventType}</td>
            <td>${(event.afterData.order_id || event.beforeData.order_id) || 'N/A'}</td>
            <td>${(event.afterData.user_id || event.beforeData.user_id) || 'N/A'}</td>
            <td>${statusDisplay}</td>
            <td>${new Date(event.timestamp).toLocaleTimeString()}</td>
        `;
        tableBody.appendChild(row);
    });
    console.log('Table now has', tableBody.rows.length, 'rows');
}

// ========== Read-Write Splitting Functions ==========

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
        
        const writeCountEl = document.getElementById('writeDbRecordCount');
        const readCountEl = document.getElementById('readDbRecordCount');
        if (writeCountEl) writeCountEl.textContent = status.writeRecordCount || 0;
        if (readCountEl) readCountEl.textContent = status.readRecordCount || 0;
        
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
        const writeResponse = await fetch(`${API_BASE}/writeData`);
        const writeResult = await writeResponse.json();
        
        if (writeResult.success) {
            renderWriteDbTable(writeResult.data || []);
            const writeCountEl = document.getElementById('writeDbCount');
            if (writeCountEl) writeCountEl.textContent = writeResult.data.length;
        }
        
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

async function insertToWriteDb() {
    try {
        const userId = document.getElementById('rwUserId').value;
        const status = document.getElementById('rwOrderStatus').value;
        const feedbackEl = document.getElementById('rwInsertFeedback');
        
        if (!userId || !status) {
            feedbackEl.textContent = '❌ Please fill in all fields';
            feedbackEl.style.color = '#ef4444';
            setTimeout(() => feedbackEl.textContent = '', 3000);
            return;
        }
        
        const response = await fetch(`${API_BASE}/replication/insertWrite?userId=${userId}&status=${status}`, {
            method: 'POST'
        });
        const result = await response.json();
        
        if (result.success) {
            feedbackEl.textContent = '✅ Inserted to Write DB!';
            feedbackEl.style.color = '#10b981';
            
            // Refresh the write DB table immediately
            refreshReadWriteData();
            pollReplicationStatus();
            
            setTimeout(() => feedbackEl.textContent = '', 3000);
        } else {
            feedbackEl.textContent = '❌ Error: ' + result.message;
            feedbackEl.style.color = '#ef4444';
            setTimeout(() => feedbackEl.textContent = '', 5000);
        }
    } catch (error) {
        console.error('Error inserting to write DB:', error);
        const feedbackEl = document.getElementById('rwInsertFeedback');
        feedbackEl.textContent = '❌ Failed to insert';
        feedbackEl.style.color = '#ef4444';
        setTimeout(() => feedbackEl.textContent = '', 5000);
    }
}

async function searchReadDatabase() {
    try {
        const searchType = document.getElementById('rwSearchType').value;
        const searchValue = document.getElementById('rwSearchValue').value;
        const feedbackEl = document.getElementById('rwSearchFeedback');
        const resultsDiv = document.getElementById('rwSearchResults');
        
        if (!searchValue) {
            feedbackEl.textContent = '❌ Please enter a search value';
            feedbackEl.style.color = '#ef4444';
            setTimeout(() => feedbackEl.textContent = '', 3000);
            return;
        }
        
        feedbackEl.textContent = '🔍 Searching Read DB...';
        feedbackEl.style.color = '#3b82f6';
        
        const response = await fetch(`${API_BASE}/replication/searchRead?${searchType}=${searchValue}`);
        const result = await response.json();
        
        if (result.success) {
            const records = result.data || [];
            feedbackEl.textContent = `✅ Found ${records.length} record(s) in Read DB`;
            feedbackEl.style.color = '#10b981';
            
            renderSearchResults(records);
            resultsDiv.style.display = 'block';
            
            setTimeout(() => feedbackEl.textContent = '', 3000);
        } else {
            feedbackEl.textContent = '❌ Search failed: ' + result.message;
            feedbackEl.style.color = '#ef4444';
            setTimeout(() => feedbackEl.textContent = '', 5000);
        }
    } catch (error) {
        console.error('Error searching read DB:', error);
        const feedbackEl = document.getElementById('rwSearchFeedback');
        feedbackEl.textContent = '❌ Search failed';
        feedbackEl.style.color = '#ef4444';
        setTimeout(() => feedbackEl.textContent = '', 5000);
    }
}

function renderSearchResults(records) {
    const tableBody = document.getElementById('rwSearchTable');
    const countEl = document.getElementById('rwSearchCount');
    
    tableBody.innerHTML = '';
    countEl.textContent = `${records.length} found`;
    
    if (records.length === 0) {
        tableBody.innerHTML = '<tr><td colspan="3" style="text-align: center; color: #666;">No records found in Read Database</td></tr>';
        return;
    }
    
    records.forEach(record => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td><strong>${record.order_id}</strong></td>
            <td>${record.user_id}</td>
            <td><span style="background: #e0e7ff; padding: 4px 8px; border-radius: 4px;">${record.status}</span></td>
        `;
        tableBody.appendChild(row);
    });
}

function clearSearch() {
    document.getElementById('rwSearchValue').value = '';
    document.getElementById('rwSearchResults').style.display = 'none';
    document.getElementById('rwSearchFeedback').textContent = '';
}

// ==================== Migration Functions ====================

let migrationRefreshInterval = null;

async function setupMigrationSource() {
    const feedbackEl = document.getElementById('migrationFeedback');
    const setupBtn = document.getElementById('migrationSetupBtn');
    
    if (setupBtn) setupBtn.disabled = true;
    if (feedbackEl) {
        feedbackEl.textContent = '⏳ Setting up source database...';
        feedbackEl.style.background = '#eff6ff';
        feedbackEl.style.color = '#1e40af';
    }
    
    try {
        const response = await fetch(`${API_BASE}/migration/setup`, { method: 'POST' });
        const result = await response.json();
        
        if (result.success) {
            if (feedbackEl) {
                feedbackEl.textContent = `✅ ${result.message} (${result.recordCount} records)`;
                feedbackEl.style.background = '#d1fae5';
                feedbackEl.style.color = '#065f46';
            }
            refreshMigrationData();
        } else {
            if (feedbackEl) {
                feedbackEl.textContent = `❌ ${result.message}`;
                feedbackEl.style.background = '#fee2e2';
                feedbackEl.style.color = '#991b1b';
            }
        }
    } catch (error) {
        if (feedbackEl) {
            feedbackEl.textContent = `❌ Error: ${error.message}`;
            feedbackEl.style.background = '#fee2e2';
            feedbackEl.style.color = '#991b1b';
        }
    } finally {
        if (setupBtn) setupBtn.disabled = false;
        if (feedbackEl) setTimeout(() => feedbackEl.textContent = '', 5000);
    }
}

async function startMigration() {
    const feedbackEl = document.getElementById('migrationFeedback');
    const startBtn = document.getElementById('migrationStartBtn');
    const stopBtn = document.getElementById('migrationStopBtn');
    
    if (startBtn) startBtn.disabled = true;
    if (feedbackEl) {
        feedbackEl.textContent = '⏳ Checking source database...';
        feedbackEl.style.background = '#eff6ff';
        feedbackEl.style.color = '#1e40af';
    }
    
    try {
        // First check if source database has data
        const sourceStatusResponse = await fetch(`${API_BASE}/migration/status`);
        const sourceStatus = await sourceStatusResponse.json();
        
        if (!sourceStatus.sourceCount || sourceStatus.sourceCount === 0) {
            if (feedbackEl) {
                feedbackEl.textContent = '⚠️ Source database is empty! Click "Setup Source DB" first.';
                feedbackEl.style.background = '#fef3c7';
                feedbackEl.style.color = '#92400e';
            }
            if (startBtn) startBtn.disabled = false;
            return;
        }
        
        if (feedbackEl) {
            feedbackEl.textContent = `⏳ Found ${sourceStatus.sourceCount} records. Stopping any existing CDC...`;
        }
        
        // Stop any existing CDC stream first (ignore errors)
        try {
            await fetch(`${API_BASE}/stopStreaming`, { method: 'POST' });
            await new Promise(resolve => setTimeout(resolve, 1000)); // Wait 1 second
        } catch (e) {
            console.log('No existing CDC to stop:', e);
        }
        
        if (feedbackEl) {
            feedbackEl.textContent = `⏳ Preparing target database...`;
        }
        
        // Prepare target database
        await fetch(`${API_BASE}/migration/prepare`, { method: 'POST' });
        
        if (feedbackEl) {
            feedbackEl.textContent = `⏳ Copying ${sourceStatus.sourceCount} records to target (initial baseline)...`;
        }
        
        // Copy initial data from source to target
        const copyResponse = await fetch(`${API_BASE}/migration/copyInitialData`, { method: 'POST' });
        const copyResult = await copyResponse.json();
        
        if (copyResult.success) {
            console.log(`Copied ${copyResult.recordsCopied} records to target`);
        }
        
        if (feedbackEl) {
            feedbackEl.textContent = '⏳ Starting CDC streaming for ongoing changes...';
        }
        
        // Then start CDC streaming from migration_db (source_db exposed via Proxy)
        const response = await fetch(`${API_BASE}/startStreaming?database=migration_db&tables=t_order`, {
            method: 'POST'
        });
        
        // Check if response is JSON
        const contentType = response.headers.get('content-type');
        let result;
        
        if (contentType && contentType.includes('application/json')) {
            result = await response.json();
        } else {
            // Handle non-JSON response
            const text = await response.text();
            // Check if it's a success message
            if (text.includes('success') || text.includes('started') || text.includes('streaming')) {
                result = { success: true, message: text };
            } else {
                throw new Error(text || 'Failed to start CDC streaming');
            }
        }
        
        if (result.success || result.message === 'Already streaming') {
            if (feedbackEl) {
                feedbackEl.textContent = '✅ Migration CDC started! Streaming changes from Source DB → Target DB...';
                feedbackEl.style.background = '#d1fae5';
                feedbackEl.style.color = '#065f46';
            }
            if (stopBtn) stopBtn.disabled = false;
            
            // Start auto-refresh
            if (migrationRefreshInterval) clearInterval(migrationRefreshInterval);
            migrationRefreshInterval = setInterval(refreshMigrationData, 3000);
            refreshMigrationData(); // Refresh immediately
        } else {
            if (feedbackEl) {
                feedbackEl.textContent = `⚠️ ${result.message}`;
                feedbackEl.style.background = '#fef3c7';
                feedbackEl.style.color = '#92400e';
            }
        }
    } catch (error) {
        console.error('Migration start error:', error);
        if (feedbackEl) {
            feedbackEl.textContent = `❌ Error: ${error.message}`;
            feedbackEl.style.background = '#fee2e2';
            feedbackEl.style.color = '#991b1b';
        }
    } finally {
        if (startBtn) startBtn.disabled = false;
    }
}

async function stopMigration() {
    const feedbackEl = document.getElementById('migrationFeedback');
    const stopBtn = document.getElementById('migrationStopBtn');
    
    if (stopBtn) stopBtn.disabled = true;
    if (feedbackEl) {
        feedbackEl.textContent = '⏳ Stopping migration...';
        feedbackEl.style.background = '#eff6ff';
        feedbackEl.style.color = '#1e40af';
    }
    
    try {
        const response = await fetch(`${API_BASE}/stop`, { method: 'POST' });
        const result = await response.json();
        
        if (feedbackEl) {
            feedbackEl.textContent = '✅ Migration paused';
            feedbackEl.style.background = '#fef3c7';
            feedbackEl.style.color = '#92400e';
        }
        
        if (migrationRefreshInterval) {
            clearInterval(migrationRefreshInterval);
            migrationRefreshInterval = null;
        }
    } catch (error) {
        if (feedbackEl) {
            feedbackEl.textContent = `❌ Error: ${error.message}`;
            feedbackEl.style.background = '#fee2e2';
            feedbackEl.style.color = '#991b1b';
        }
    } finally {
        setTimeout(() => {
            if (feedbackEl) feedbackEl.textContent = '';
            if (stopBtn) stopBtn.disabled = false;
        }, 5000);
    }
}

async function verifyMigration() {
    const feedbackEl = document.getElementById('migrationFeedback');
    
    if (feedbackEl) {
        feedbackEl.textContent = '⏳ Verifying synchronization...';
        feedbackEl.style.background = '#eff6ff';
        feedbackEl.style.color = '#1e40af';
    }
    
    try {
        const response = await fetch(`${API_BASE}/migration/verify`);
        const result = await response.json();
        
        if (result.inSync) {
            if (feedbackEl) {
                feedbackEl.textContent = `✅ ${result.message}`;
                feedbackEl.style.background = '#d1fae5';
                feedbackEl.style.color = '#065f46';
            }
        } else {
            if (feedbackEl) {
                feedbackEl.textContent = `⚠️ ${result.message}`;
                feedbackEl.style.background = '#fef3c7';
                feedbackEl.style.color = '#92400e';
            }
        }
        
        refreshMigrationData();
    } catch (error) {
        if (feedbackEl) {
            feedbackEl.textContent = `❌ Error: ${error.message}`;
            feedbackEl.style.background = '#fee2e2';
            feedbackEl.style.color = '#991b1b';
        }
    } finally {
        if (feedbackEl) setTimeout(() => feedbackEl.textContent = '', 5000);
    }
}

async function refreshMigrationData() {
    try {
        // Get status
        const statusResponse = await fetch(`${API_BASE}/migration/status`);
        const status = await statusResponse.json();
        
        // Update counters (with null checks for merged tab)
        const migrationSourceCountEl = document.getElementById('migrationSourceCount');
        if (migrationSourceCountEl) migrationSourceCountEl.textContent = status.sourceCount;
        
        const migrationTargetCountEl = document.getElementById('migrationTargetCount');
        if (migrationTargetCountEl) migrationTargetCountEl.textContent = status.targetCount;
        
        const migrationLagEl = document.getElementById('migrationLag');
        if (migrationLagEl) migrationLagEl.textContent = status.lag;
        
        const migrationProgressEl = document.getElementById('migrationProgress');
        if (migrationProgressEl) migrationProgressEl.textContent = status.progress + '%';
        
        const migrationProgressPercentEl = document.getElementById('migrationProgressPercent');
        if (migrationProgressPercentEl) migrationProgressPercentEl.textContent = status.progress + '%';
        
        // Update progress bar (if exists)
        const progressBar = document.getElementById('migrationProgressBar');
        if (progressBar) {
            progressBar.style.width = status.progress + '%';
            if (status.progress > 10) {
                progressBar.textContent = status.progress + '%';
            }
        }
        
        // Update status text (if exists)
        const statusTextEl = document.getElementById('migrationStatusText');
        if (statusTextEl) {
            statusTextEl.textContent = status.status;
            
            if (status.synchronized) {
                statusTextEl.style.background = '#d1fae5';
                statusTextEl.style.color = '#065f46';
            } else if (status.progress > 0) {
                statusTextEl.style.background = '#fef3c7';
                statusTextEl.style.color = '#92400e';
            }
        }
        
        // Get source records
        const sourceResponse = await fetch(`${API_BASE}/migration/source/records`);
        const sourceRecords = await sourceResponse.json();
        renderMigrationSourceTable(sourceRecords);
        
        const migrationSourceBadgeEl = document.getElementById('migrationSourceBadge');
        if (migrationSourceBadgeEl) migrationSourceBadgeEl.textContent = sourceRecords.length + ' records';
        
        // Get target records - Note: target is already being fetched by fetchDataComparison
        // So we don't need to render it again here, just update the badge
        const targetResponse = await fetch(`${API_BASE}/migration/target/records`);
        const targetRecords = await targetResponse.json();
        // Don't call renderMigrationTargetTable - it's the same as targetDataTable
        
        const migrationTargetBadgeEl = document.getElementById('migrationTargetBadge');
        if (migrationTargetBadgeEl) migrationTargetBadgeEl.textContent = targetRecords.length + ' records';
        
    } catch (error) {
        console.error('Error refreshing migration data:', error);
    }
}

function renderMigrationSourceTable(records) {
    const tableBody = document.getElementById('migrationSourceTable');
    if (!tableBody) return;
    
    tableBody.innerHTML = '';
    
    if (records.length === 0) {
        tableBody.innerHTML = '<tr><td colspan="3" style="text-align: center; color: #666;">No records in source database</td></tr>';
        return;
    }
    
    records.forEach(record => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${record.order_id}</td>
            <td>${record.user_id}</td>
            <td><span style="background: #dbeafe; padding: 4px 8px; border-radius: 4px;">${record.status}</span></td>
        `;
        tableBody.appendChild(row);
    });
}

function renderMigrationTargetTable(records) {
    const tableBody = document.getElementById('migrationTargetTable');
    if (!tableBody) return;
    
    tableBody.innerHTML = '';
    
    if (records.length === 0) {
        tableBody.innerHTML = '<tr><td colspan="3" style="text-align: center; color: #666;">No records in target database</td></tr>';
        return;
    }
    
    records.forEach(record => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${record.order_id}</td>
            <td>${record.user_id}</td>
            <td><span style="background: #dcfce7; padding: 4px 8px; border-radius: 4px;">${record.status}</span></td>
        `;
        tableBody.appendChild(row);
    });
}

// ==================== Clear Database Functions ====================

async function clearShardingSphereDb() {
    if (!confirm('⚠️ Clear all data from ShardingSphere DB (sharding_db)?\n\nThis will delete all records via the Proxy.')) {
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE}/clearShardingSphereDb`, { method: 'POST' });
        const result = await response.json();
        
        if (result.success) {
            showNotification('✅ ShardingSphere DB cleared successfully!', 'success');
            // Refresh the data comparison tab
            if (window.location.hash === '#replication' || document.querySelector('.tab-content.active')?.id === 'replicationTab') {
                fetchDataComparison();
            }
        } else {
            showNotification('❌ ' + result.message, 'error');
        }
    } catch (error) {
        console.error('Error clearing ShardingSphere DB:', error);
        showNotification('❌ Failed to clear ShardingSphere DB', 'error');
    }
}

async function clearMigrationSourceDb() {
    if (!confirm('⚠️ Clear all data from Source DB (source_db)?\n\nThis will delete all records from the migration source database (port 5435).')) {
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE}/clearMigrationSourceDb`, { method: 'POST' });
        const result = await response.json();
        
        if (result.success) {
            showNotification('✅ Source DB cleared successfully!', 'success');
            // Refresh the migration data
            if (window.location.hash === '#replication' || document.querySelector('.tab-content.active')?.id === 'replicationTab') {
                refreshMigrationData();
            }
        } else {
            showNotification('❌ ' + result.message, 'error');
        }
    } catch (error) {
        console.error('Error clearing Source DB:', error);
        showNotification('❌ Failed to clear Source DB', 'error');
    }
}

async function clearTargetDb() {
    if (!confirm('⚠️ Clear all data from Target DB (target_db)?\n\nThis will delete all records from the multi-source CDC target database (port 5432).')) {
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE}/clearTargetDb`, { method: 'POST' });
        const result = await response.json();
        
        if (result.success) {
            showNotification('✅ Target DB cleared successfully!', 'success');
            // Refresh all relevant data
            if (window.location.hash === '#replication' || document.querySelector('.tab-content.active')?.id === 'replicationTab') {
                fetchDataComparison();
                refreshMigrationData();
            }
        } else {
            showNotification('❌ ' + result.message, 'error');
        }
    } catch (error) {
        console.error('Error clearing Target DB:', error);
        showNotification('❌ Failed to clear Target DB', 'error');
    }
}

function showNotification(message, type = 'info') {
    // Create notification element
    const notification = document.createElement('div');
    notification.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        padding: 15px 25px;
        background: ${type === 'success' ? '#10b981' : type === 'error' ? '#ef4444' : '#3b82f6'};
        color: white;
        border-radius: 8px;
        box-shadow: 0 4px 15px rgba(0,0,0,0.3);
        z-index: 10000;
        font-weight: 600;
        animation: slideIn 0.3s ease-out;
    `;
    notification.textContent = message;
    
    // Add animation keyframes
    if (!document.getElementById('notificationStyles')) {
        const style = document.createElement('style');
        style.id = 'notificationStyles';
        style.textContent = `
            @keyframes slideIn {
                from { transform: translateX(400px); opacity: 0; }
                to { transform: translateX(0); opacity: 1; }
            }
            @keyframes slideOut {
                from { transform: translateX(0); opacity: 1; }
                to { transform: translateX(400px); opacity: 0; }
            }
        `;
        document.head.appendChild(style);
    }
    
    document.body.appendChild(notification);
    
    // Auto-remove after 3 seconds
    setTimeout(() => {
        notification.style.animation = 'slideOut 0.3s ease-in';
        setTimeout(() => notification.remove(), 300);
    }, 3000);
}
