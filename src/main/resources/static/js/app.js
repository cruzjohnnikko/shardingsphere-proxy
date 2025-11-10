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
});

// Setup Event Listeners
function setupEventListeners() {
    connectBtn.addEventListener('click', connect);
    startStreamingBtn.addEventListener('click', startStreaming);
    stopBtn.addEventListener('click', stop);
    resetBtn.addEventListener('click', resetStatistics);
    startGeneratorBtn.addEventListener('click', startGenerator);
    stopGeneratorBtn.addEventListener('click', stopGenerator);
    manualInsertBtn.addEventListener('click', manualInsert);
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
            const message = JSON.parse(event.data);
            handleWebSocketMessage(message);
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
    switch (message.type) {
        case 'event':
            addEvent(message.data);
            break;
        case 'statistics':
            updateStatistics(message.data);
            break;
        case 'status':
            addMessage(message.message, 'info');
            break;
        case 'error':
            addMessage(message.message, 'error');
            break;
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
        connectBtn.disabled = true;
        const response = await fetch(`${API_BASE}/connect`, { method: 'POST' });
        const result = await response.json();
        
        if (result.success) {
            addMessage(result.message, 'success');
            startStreamingBtn.disabled = false;
            stopBtn.disabled = true;
        } else {
            addMessage(result.message, 'error');
            connectBtn.disabled = false;
            startStreamingBtn.disabled = true;
            stopBtn.disabled = true;
        }
    } catch (error) {
        addMessage('Failed to connect: ' + error.message, 'error');
        connectBtn.disabled = false;
        startStreamingBtn.disabled = true;
        stopBtn.disabled = true;
    }
}

async function startStreaming() {
    try {
        startStreamingBtn.disabled = true;
        stopBtn.disabled = false;
        connectBtn.disabled = true;
        
        const response = await fetch(`${API_BASE}/startStreaming`, { method: 'POST' });
        const result = await response.text();
        addMessage(result, 'success');
    } catch (error) {
        addMessage('Failed to start streaming: ' + error.message, 'error');
        startStreamingBtn.disabled = false;
        stopBtn.disabled = true;
        connectBtn.disabled = false;
    }
}

async function stop() {
    try {
        stopBtn.disabled = true;
        startStreamingBtn.disabled = false;
        
        const response = await fetch(`${API_BASE}/stopStreaming`, { method: 'POST' });
        const result = await response.text();
        addMessage(result, 'success');
    } catch (error) {
        addMessage('Failed to stop: ' + error.message, 'error');
        stopBtn.disabled = false;
        startStreamingBtn.disabled = true;
    }
}

async function resetStatistics() {
    try {
        const clearData = clearDataCheckbox.checked;
        const clearTarget = clearTargetCheckbox.checked;
        
        // Confirm if clearing data
        if (clearData || clearTarget) {
            let message = '⚠️ Warning: This will delete data from:\n';
            if (clearData) message += `- Source database (${recordCountEl.textContent} records)\n`;
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
            recordCountEl.textContent = result.count.toLocaleString();
        } else {
            recordCountEl.textContent = 'Error';
        }
    } catch (error) {
        console.error('Failed to fetch record count:', error);
        recordCountEl.textContent = '?';
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
    if (status.streaming) {
        cdcStatusEl.textContent = 'Streaming';
        cdcStatusEl.className = 'status-badge streaming';
        // Update button states
        startStreamingBtn.disabled = true;
        stopBtn.disabled = false;
        connectBtn.disabled = true;
    } else if (status.connected) {
        cdcStatusEl.textContent = 'Connected';
        cdcStatusEl.className = 'status-badge connected';
        // Update button states
        startStreamingBtn.disabled = false;
        stopBtn.disabled = true;
        connectBtn.disabled = true;
    } else {
        cdcStatusEl.textContent = 'Disconnected';
        cdcStatusEl.className = 'status-badge disconnected';
        // Update button states
        startStreamingBtn.disabled = true;
        stopBtn.disabled = true;
        connectBtn.disabled = false;
    }
}

function updateGeneratorStatus(running) {
    if (running) {
        generatorStatusEl.textContent = 'Running';
        generatorStatusEl.className = 'status-badge running';
    } else {
        generatorStatusEl.textContent = 'Stopped';
        generatorStatusEl.className = 'status-badge stopped';
    }
}

function updateStatistics(stats) {
    totalEventsEl.textContent = formatNumber(stats.totalEvents);
    insertCountEl.textContent = formatNumber(stats.insertCount);
    updateCountEl.textContent = formatNumber(stats.updateCount);
    deleteCountEl.textContent = formatNumber(stats.deleteCount);
    eventsPerSecondEl.textContent = stats.eventsPerSecond.toFixed(2);
    uptimeEl.textContent = formatUptime(stats.uptime);
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

function addMessage(message, type) {
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
            
            // Start/Stop comparison data refresh based on active tab
            if (targetTab === 'comparison') {
                fetchDataComparison(); // Load immediately
                if (!comparisonRefreshInterval) {
                    comparisonRefreshInterval = setInterval(fetchDataComparison, 5000);
                }
            } else {
                if (comparisonRefreshInterval) {
                    clearInterval(comparisonRefreshInterval);
                    comparisonRefreshInterval = null;
                }
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
        renderCdcEventsTable(cdcEvents || []);

        // Calculate Unsynced Records (only positive values make sense)
        // Unsynced = source records that haven't been synced to target yet
        // If target has more records than source (shouldn't happen normally), unsynced = 0
        const sourceCount = Array.isArray(sourceRecords) ? sourceRecords.length : 0;
        const targetCount = Array.isArray(targetRecords) ? targetRecords.length : 0;
        let unsyncedCount = Math.max(0, sourceCount - targetCount);
        document.getElementById('unsyncedRecordCount').textContent = unsyncedCount.toLocaleString();

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
        console.error('Error stack:', error.stack);
        // Set defaults instead of Error
        const safeSetText = (id, defaultValue) => {
            const el = document.getElementById(id);
            if (el) el.textContent = defaultValue;
        };
        safeSetText('sourceRecordCount', '0');
        safeSetText('targetRecordCount', '0');
        safeSetText('unsyncedRecordCount', '0');
        safeSetText('totalEventsCaptured', '0');
        safeSetText('cdcJobStatus', 'Not Running');
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
    const tableBody = document.getElementById('cdcEventsTable');
    tableBody.innerHTML = '';
    
    if (events.length === 0) {
        tableBody.innerHTML = '<tr><td colspan="5" style="text-align: center; color: #666;">No CDC events captured yet.</td></tr>';
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
}
