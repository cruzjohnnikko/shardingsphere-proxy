#!/bin/bash

# CDC Verification Script
# This script checks the complete CDC setup and verifies everything is working

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
API_BASE="http://localhost:8080/api/cdc"
CDC_APP_URL="http://localhost:8080"

echo "=================================="
echo "🔍 CDC Verification Report"
echo "=================================="
echo ""

# Function to print section header
print_header() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

# Function to check if service is running
check_service() {
    if curl -s "$1" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Running${NC}"
        return 0
    else
        echo -e "${RED}✗ Not Running${NC}"
        return 1
    fi
}

# 1. Check Services Status
print_header "1. 🚀 Services Status"

echo -n "CDC Application (Port 8080): "
if check_service "$CDC_APP_URL"; then
    APP_RUNNING=true
else
    APP_RUNNING=false
fi

echo -n "ShardingSphere Proxy (Port 3308): "
if timeout 2 bash -c "</dev/tcp/localhost/3308" 2>/dev/null; then
    echo -e "${GREEN}✓ Running${NC}"
    PROXY_RUNNING=true
else
    echo -e "${RED}✗ Not Running${NC}"
    PROXY_RUNNING=false
fi

echo -n "PostgreSQL Database (Port 5432): "
if timeout 2 bash -c "</dev/tcp/localhost/5432" 2>/dev/null; then
    echo -e "${GREEN}✓ Running${NC}"
    PG_RUNNING=true
else
    echo -e "${RED}✗ Not Running${NC}"
    PG_RUNNING=false
fi

# Exit if CDC app is not running
if [ "$APP_RUNNING" = false ]; then
    echo ""
    echo -e "${RED}❌ CDC Application is not running. Please start it first.${NC}"
    echo "Run: java -jar target/shardingsphere-proxy-cdc-demo-1.0-SNAPSHOT.jar"
    exit 1
fi

# 2. Source Database (Physical PostgreSQL)
print_header "2. 📊 Source Database (Physical PostgreSQL - Port 5432)"

if [ "$PG_RUNNING" = true ]; then
    SOURCE_COUNT=$(podman-compose exec -T postgres psql -U postgres -d migration_ds_0 -t -c "SELECT COUNT(*) FROM t_order;" 2>/dev/null | tr -d ' ')
    
    if [ -n "$SOURCE_COUNT" ]; then
        echo -e "Database:       ${YELLOW}migration_ds_0${NC}"
        echo -e "Table:          ${YELLOW}t_order${NC}"
        echo -e "Record Count:   ${GREEN}${SOURCE_COUNT}${NC}"
        
        # Get min/max order_id
        SOURCE_RANGE=$(podman-compose exec -T postgres psql -U postgres -d migration_ds_0 -t -c "SELECT COALESCE(MIN(order_id), 0) || '-' || COALESCE(MAX(order_id), 0) FROM t_order;" 2>/dev/null | tr -d ' ')
        echo -e "Order ID Range: ${GREEN}${SOURCE_RANGE}${NC}"
        
        # Get status distribution (top 5)
        echo -e "\nStatus Distribution:"
        podman-compose exec -T postgres psql -U postgres -d migration_ds_0 -c "SELECT status, COUNT(*) as count FROM t_order GROUP BY status ORDER BY count DESC LIMIT 5;" 2>/dev/null | grep -v "^-" | grep -v "^(" | tail -n +2 | head -n 5 | while read line; do
            if [ -n "$line" ]; then
                echo "  $line"
            fi
        done
    else
        echo -e "${YELLOW}⚠ Could not query source database${NC}"
    fi
else
    echo -e "${YELLOW}⚠ PostgreSQL not accessible${NC}"
fi

# 3. CDC Job Status
print_header "3. 🔄 CDC Job Status"

CDC_STATUS=$(curl -s "${API_BASE}/status" 2>/dev/null)

if [ -n "$CDC_STATUS" ]; then
    CONNECTED=$(echo "$CDC_STATUS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(str(d['connected']).lower())" 2>/dev/null)
    STREAMING=$(echo "$CDC_STATUS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(str(d['streaming']).lower())" 2>/dev/null)
    STREAMING_ID=$(echo "$CDC_STATUS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['streamingId'] if d['streamingId'] else 'N/A')" 2>/dev/null)
    EVENTS_RECEIVED=$(echo "$CDC_STATUS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['eventsReceived'])" 2>/dev/null)
    START_TIME=$(echo "$CDC_STATUS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['startTime'])" 2>/dev/null)
    ERROR_MSG=$(echo "$CDC_STATUS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['errorMessage'] if d['errorMessage'] else 'None')" 2>/dev/null)
    
    if [ "$CONNECTED" = "true" ]; then
        echo -e "Connected:       ${GREEN}✓ Yes${NC}"
    else
        echo -e "Connected:       ${RED}✗ No${NC}"
    fi
    
    if [ "$STREAMING" = "true" ]; then
        echo -e "Streaming:       ${GREEN}✓ Active${NC}"
        echo -e "Streaming ID:    ${YELLOW}${STREAMING_ID}${NC}"
        
        # Calculate uptime if streaming
        if [ "$START_TIME" != "0" ]; then
            CURRENT_TIME=$(date +%s)
            START_TIME_SEC=$((START_TIME / 1000))
            UPTIME_SEC=$((CURRENT_TIME - START_TIME_SEC))
            UPTIME_MIN=$((UPTIME_SEC / 60))
            UPTIME_SEC_REMAINING=$((UPTIME_SEC % 60))
            echo -e "Uptime:          ${GREEN}${UPTIME_MIN}m ${UPTIME_SEC_REMAINING}s${NC}"
        fi
    else
        echo -e "Streaming:       ${RED}✗ Inactive${NC}"
        echo -e "Streaming ID:    ${YELLOW}${STREAMING_ID}${NC}"
    fi
    
    echo -e "Events Received: ${GREEN}${EVENTS_RECEIVED}${NC}"
    
    if [ "$ERROR_MSG" != "None" ]; then
        echo -e "Error:           ${RED}${ERROR_MSG}${NC}"
    else
        echo -e "Error:           ${GREEN}${ERROR_MSG}${NC}"
    fi
else
    echo -e "${RED}✗ Could not retrieve CDC status${NC}"
fi

# 4. Target (CDC Events Captured)
print_header "4. 🎯 Target (CDC Events Captured)"

CDC_STATS=$(curl -s "${API_BASE}/statistics" 2>/dev/null)

if [ -n "$CDC_STATS" ]; then
    TOTAL_EVENTS=$(echo "$CDC_STATS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['totalEvents'])" 2>/dev/null)
    INSERT_COUNT=$(echo "$CDC_STATS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['insertCount'])" 2>/dev/null)
    UPDATE_COUNT=$(echo "$CDC_STATS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['updateCount'])" 2>/dev/null)
    DELETE_COUNT=$(echo "$CDC_STATS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['deleteCount'])" 2>/dev/null)
    EVENTS_PER_SEC=$(echo "$CDC_STATS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f\"{d['eventsPerSecond']:.2f}\")" 2>/dev/null)
    
    echo -e "Total Events:    ${GREEN}${TOTAL_EVENTS}${NC}"
    echo -e "  ➕ Inserts:     ${GREEN}${INSERT_COUNT}${NC}"
    echo -e "  ✏️  Updates:     ${BLUE}${UPDATE_COUNT}${NC}"
    echo -e "  🗑️  Deletes:     ${RED}${DELETE_COUNT}${NC}"
    echo -e "Events/sec:      ${YELLOW}${EVENTS_PER_SEC}${NC}"
    
    # Calculate net change
    NET_CHANGE=$((INSERT_COUNT - DELETE_COUNT))
    echo ""
    echo -e "Net Change:      ${YELLOW}${NET_CHANGE}${NC} (Inserts - Deletes)"
else
    echo -e "${RED}✗ Could not retrieve CDC statistics${NC}"
fi

# 5. Data Generator Status
print_header "5. ⚙️  Data Generator Status"

GEN_STATUS=$(curl -s "${API_BASE}/generator/status" 2>/dev/null)

if [ -n "$GEN_STATUS" ]; then
    GEN_RUNNING=$(echo "$GEN_STATUS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(str(d['running']).lower())" 2>/dev/null)
    
    if [ "$GEN_RUNNING" = "true" ]; then
        echo -e "Status:          ${GREEN}✓ Running${NC}"
    else
        echo -e "Status:          ${YELLOW}○ Stopped${NC}"
    fi
else
    echo -e "${RED}✗ Could not retrieve generator status${NC}"
fi

# 6. Database Record Count via API
print_header "6. 💾 Database Record Count (via API)"

RECORD_COUNT=$(curl -s "${API_BASE}/recordCount" 2>/dev/null)

if [ -n "$RECORD_COUNT" ]; then
    DB_COUNT=$(echo "$RECORD_COUNT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['count'])" 2>/dev/null)
    echo -e "Current Count:   ${GREEN}${DB_COUNT}${NC}"
else
    echo -e "${RED}✗ Could not retrieve record count${NC}"
fi

# 7. Verification Summary
print_header "7. ✅ Verification Summary"

echo ""
if [ -n "$SOURCE_COUNT" ] && [ -n "$TOTAL_EVENTS" ] && [ -n "$NET_CHANGE" ]; then
    echo "Source Database Records:     $SOURCE_COUNT"
    echo "CDC Events Captured:         $TOTAL_EVENTS"
    echo "Net Change (Insert - Delete): $NET_CHANGE"
    echo ""
    
    if [ "$STREAMING" = "true" ]; then
        echo -e "${GREEN}✓ CDC is streaming${NC}"
        
        if [ "$TOTAL_EVENTS" -gt 0 ]; then
            echo -e "${GREEN}✓ CDC has captured $TOTAL_EVENTS events${NC}"
            echo ""
            echo -e "${BLUE}ℹ️  Note:${NC} CDC only captures NEW changes after streaming starts."
            echo "   Source records ($SOURCE_COUNT) include old data before CDC started."
            echo "   To verify CDC is working, generate new data and watch events increase."
        else
            echo -e "${YELLOW}⚠ CDC is streaming but has not captured any events yet${NC}"
            echo "   Try generating some data to test CDC capture."
        fi
    else
        echo -e "${YELLOW}⚠ CDC is not streaming${NC}"
        echo "   Click 'Connect' then 'Start Streaming' in the dashboard."
    fi
else
    echo -e "${RED}✗ Could not perform complete verification${NC}"
fi

# 8. Quick Actions
print_header "8. 🔧 Quick Actions"

echo ""
echo "Dashboard:           $CDC_APP_URL"
echo ""
echo "Commands:"
echo "  # Connect CDC:     curl -X POST ${API_BASE}/connect"
echo "  # Start Streaming: curl -X POST ${API_BASE}/start"
echo "  # Start Generator: curl -X POST '${API_BASE}/generator/start?interval=1000&operation=MIXED'"
echo "  # Stop Generator:  curl -X POST ${API_BASE}/generator/stop"
echo "  # Reset Stats:     curl -X POST ${API_BASE}/reset"
echo "  # Clear Database:  curl -X POST ${API_BASE}/clearData"
echo ""

# 9. Real-Time Test Suggestion
if [ "$STREAMING" = "true" ] && [ "$GEN_RUNNING" = "false" ]; then
    print_header "9. 🧪 Real-Time Test (Optional)"
    echo ""
    echo "Test CDC capture in real-time:"
    echo ""
    echo "  # Start generator for 10 seconds"
    echo "  curl -X POST '${API_BASE}/generator/start?interval=1000&operation=INSERT'"
    echo "  sleep 10"
    echo "  curl -X POST ${API_BASE}/generator/stop"
    echo "  ./verify-cdc.sh"
    echo ""
    echo "Expected: CDC Events should increase by ~10"
    echo ""
fi

echo "=================================="
echo "✓ Verification Complete"
echo "=================================="
echo ""

