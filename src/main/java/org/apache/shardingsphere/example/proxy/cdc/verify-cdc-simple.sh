#!/bin/bash

# Simple CDC Verification Script
# Shows only essential CDC information

set -e

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

API_BASE="http://localhost:8080/api/cdc"

echo ""
echo "╔════════════════════════════════════════════════╗"
echo "║         CDC Verification Report                ║"
echo "╚════════════════════════════════════════════════╝"
echo ""

# Get Source DB Count via API (goes through proxy automatically)
RECORD_COUNT_RESPONSE=$(curl -s "${API_BASE}/recordCount" 2>/dev/null)
SOURCE_COUNT=$(echo "$RECORD_COUNT_RESPONSE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['count'])" 2>/dev/null)
if [ -z "$SOURCE_COUNT" ]; then
    SOURCE_COUNT="N/A"
fi

# Get CDC Status
CDC_STATUS=$(curl -s "${API_BASE}/status" 2>/dev/null)
CONNECTED=$(echo "$CDC_STATUS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(str(d['connected']).lower())" 2>/dev/null)
STREAMING=$(echo "$CDC_STATUS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(str(d['streaming']).lower())" 2>/dev/null)
STREAMING_ID=$(echo "$CDC_STATUS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['streamingId'] if d['streamingId'] else 'N/A')" 2>/dev/null)

# Get CDC Statistics
CDC_STATS=$(curl -s "${API_BASE}/statistics" 2>/dev/null)
TOTAL_EVENTS=$(echo "$CDC_STATS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['totalEvents'])" 2>/dev/null)
INSERT_COUNT=$(echo "$CDC_STATS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['insertCount'])" 2>/dev/null)
UPDATE_COUNT=$(echo "$CDC_STATS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['updateCount'])" 2>/dev/null)
DELETE_COUNT=$(echo "$CDC_STATS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['deleteCount'])" 2>/dev/null)

# Calculate Net Change (Inserts - Deletes)
if [ -n "$INSERT_COUNT" ] && [ -n "$DELETE_COUNT" ]; then
    NET_CHANGE=$((INSERT_COUNT - DELETE_COUNT))
else
    NET_CHANGE="N/A"
fi

# Calculate Unsynced Records
if [ "$SOURCE_COUNT" != "N/A" ] && [ "$NET_CHANGE" != "N/A" ]; then
    UNSYNCED=$((SOURCE_COUNT - NET_CHANGE))
else
    UNSYNCED="N/A"
fi

# Display Results
echo -e "${CYAN}┌─ Database Records${NC}"
echo -e "│"
echo -e "│  Source DB (via Proxy):     ${GREEN}${SOURCE_COUNT}${NC} records"
echo -e "│  Target DB (CDC Captured):  ${GREEN}${NET_CHANGE}${NC} records (Inserts - Deletes)"
echo -e "│  Unsynced Records:          ${YELLOW}${UNSYNCED}${NC} records"
echo -e "│"

echo -e "${CYAN}├─ CDC Status${NC}"
echo -e "│"
if [ "$STREAMING" = "true" ]; then
    echo -e "│  CDC Status:                ${GREEN}✓ Running${NC}"
else
    echo -e "│  CDC Status:                ${RED}✗ Not Running${NC}"
fi
echo -e "│  Job ID:                    ${BLUE}${STREAMING_ID}${NC}"
echo -e "│"

echo -e "${CYAN}├─ Event Breakdown${NC}"
echo -e "│"
echo -e "│  Total Events Captured:     ${GREEN}${TOTAL_EVENTS}${NC}"
echo -e "│    ➕ Inserts:               ${GREEN}${INSERT_COUNT}${NC}"
echo -e "│    ✏️  Updates:               ${BLUE}${UPDATE_COUNT}${NC}"
echo -e "│    🗑️  Deletes:               ${RED}${DELETE_COUNT}${NC}"
echo -e "│"

echo -e "${CYAN}└─ Summary${NC}"
echo ""

# Interpretation
if [ "$UNSYNCED" != "N/A" ]; then
    if [ "$UNSYNCED" -eq 0 ]; then
        echo -e "  ${GREEN}✓ All source records are synced to target${NC}"
    elif [ "$UNSYNCED" -gt 0 ]; then
        echo -e "  ${YELLOW}ℹ️  ${UNSYNCED} source records existed before CDC started${NC}"
        echo -e "  ${YELLOW}   (CDC only captures new changes after streaming starts)${NC}"
    else
        echo -e "  ${YELLOW}ℹ️  Net change calculation: ${NET_CHANGE} records${NC}"
    fi
else
    echo -e "  ${RED}✗ Could not calculate sync status${NC}"
fi

echo ""
echo "╚════════════════════════════════════════════════╝"
echo ""

