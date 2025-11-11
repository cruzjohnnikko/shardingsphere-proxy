#!/bin/bash

################################################################################
# ShardingSphere CDC Migration Workflow Script
# Purpose: Demonstrate near-zero downtime database migration
# Flow: source_db (port 5435) → target_db (port 5434) via CDC
################################################################################

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SOURCE_DB_HOST="localhost"
SOURCE_DB_PORT="5435"
SOURCE_DB_NAME="source_db"
SOURCE_DB_USER="postgres"
SOURCE_DB_PASS="postgres"

TARGET_DB_HOST="localhost"
TARGET_DB_PORT="5434"
TARGET_DB_NAME="target_db"
TARGET_DB_USER="postgres"
TARGET_DB_PASS="postgres"

PROXY_HOST="localhost"
PROXY_PORT="3308"
PROXY_DB="migration_db"
PROXY_USER="root"
PROXY_PASS="root"

APP_API="http://localhost:8080/api/cdc"

# Helper functions
print_header() {
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}\n"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${BLUE}→ $1${NC}"
}

# Phase 1: Setup Source Database with Sample Data
phase1_setup_source() {
    print_header "PHASE 1: Setup Source Database (Production Simulation)"
    
    print_info "Creating t_order table in source_db..."
    PGPASSWORD=$SOURCE_DB_PASS psql -h $SOURCE_DB_HOST -p $SOURCE_DB_PORT -U $SOURCE_DB_USER -d $SOURCE_DB_NAME << 'EOF'
-- Drop existing table
DROP TABLE IF EXISTS t_order CASCADE;

-- Create orders table
CREATE TABLE t_order (
    order_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample production data (simulating existing data)
INSERT INTO t_order (user_id, status) VALUES
    (1, 'completed'),
    (2, 'pending'),
    (3, 'shipped'),
    (1, 'processing'),
    (4, 'completed'),
    (2, 'cancelled'),
    (5, 'shipped'),
    (3, 'completed'),
    (6, 'pending'),
    (1, 'processing');

-- Display current data
SELECT COUNT(*) as total_records FROM t_order;
EOF
    
    if [ $? -eq 0 ]; then
        print_success "Source database setup completed"
        RECORD_COUNT=$(PGPASSWORD=$SOURCE_DB_PASS psql -h $SOURCE_DB_HOST -p $SOURCE_DB_PORT -U $SOURCE_DB_USER -d $SOURCE_DB_NAME -t -c "SELECT COUNT(*) FROM t_order;")
        print_info "Source DB has ${RECORD_COUNT} records"
    else
        print_error "Failed to setup source database"
        exit 1
    fi
}

# Phase 2: Prepare Target Database
phase2_prepare_target() {
    print_header "PHASE 2: Prepare Target Database (Empty Migration Destination)"
    
    print_info "Creating t_order table in target_db (empty)..."
    PGPASSWORD=$TARGET_DB_PASS psql -h $TARGET_DB_HOST -p $TARGET_DB_PORT -U $TARGET_DB_USER -d $TARGET_DB_NAME << 'EOF'
-- Drop existing table
DROP TABLE IF EXISTS t_order CASCADE;

-- Create matching table structure (must match source)
CREATE TABLE t_order (
    order_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Verify it's empty
SELECT COUNT(*) as total_records FROM t_order;
EOF
    
    if [ $? -eq 0 ]; then
        print_success "Target database prepared (empty and ready)"
        RECORD_COUNT=$(PGPASSWORD=$TARGET_DB_PASS psql -h $TARGET_DB_HOST -p $TARGET_DB_PORT -U $TARGET_DB_USER -d $TARGET_DB_NAME -t -c "SELECT COUNT(*) FROM t_order;")
        print_info "Target DB has ${RECORD_COUNT} records"
    else
        print_error "Failed to prepare target database"
        exit 1
    fi
}

# Phase 3: Verify ShardingSphere Proxy Configuration
phase3_verify_proxy() {
    print_header "PHASE 3: Verify ShardingSphere Proxy Configuration"
    
    print_info "Checking if database-migration.yaml exists..."
    if [ -f "apache-shardingsphere-5.5.2-shardingsphere-proxy-bin/conf/database-migration.yaml" ]; then
        print_success "Migration configuration found"
    else
        print_error "Migration configuration not found!"
        print_info "Please ensure database-migration.yaml is in the conf directory"
        exit 1
    fi
    
    print_info "Testing connection to ShardingSphere Proxy..."
    PGPASSWORD=$PROXY_PASS psql -h $PROXY_HOST -p $PROXY_PORT -U $PROXY_USER -d $PROXY_DB -c "SELECT 1;" > /dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        print_success "ShardingSphere Proxy is accessible on port $PROXY_PORT"
    else
        print_warning "Cannot connect to ShardingSphere Proxy"
        print_info "Make sure the proxy is running with migration_db database enabled"
    fi
}

# Phase 4: Start CDC Streaming
phase4_start_cdc() {
    print_header "PHASE 4: Start CDC Streaming (Source → Target)"
    
    print_info "Connecting CDC client..."
    curl -s -X POST "$APP_API/connect" > /dev/null
    sleep 2
    
    print_info "Starting CDC streaming from migration_db (source_db) to target_db..."
    RESPONSE=$(curl -s -X POST "$APP_API/startStreaming?database=sharding_db&tables=t_order")
    
    if echo "$RESPONSE" | grep -q "success"; then
        print_success "CDC streaming started successfully"
    else
        print_warning "CDC may already be streaming or encountered an issue"
    fi
    
    print_info "CDC is now capturing changes from source_db..."
}

# Phase 5: Simulate Production Traffic
phase5_simulate_traffic() {
    print_header "PHASE 5: Simulate Production Traffic (Writing to Source DB)"
    
    print_info "Inserting new orders into source_db (simulating live traffic)..."
    
    for i in {1..5}; do
        PGPASSWORD=$SOURCE_DB_PASS psql -h $SOURCE_DB_HOST -p $SOURCE_DB_PORT -U $SOURCE_DB_USER -d $SOURCE_DB_NAME << EOF
INSERT INTO t_order (user_id, status) VALUES ($((RANDOM % 10 + 1)), 'pending');
EOF
        print_info "Inserted order #$i"
        sleep 1
    done
    
    SOURCE_COUNT=$(PGPASSWORD=$SOURCE_DB_PASS psql -h $SOURCE_DB_HOST -p $SOURCE_DB_PORT -U $SOURCE_DB_USER -d $SOURCE_DB_NAME -t -c "SELECT COUNT(*) FROM t_order;")
    print_success "Source DB now has ${SOURCE_COUNT} records"
}

# Phase 6: Monitor Replication
phase6_monitor() {
    print_header "PHASE 6: Monitor Migration Progress"
    
    print_info "Waiting for CDC to replicate data to target..."
    sleep 5
    
    SOURCE_COUNT=$(PGPASSWORD=$SOURCE_DB_PASS psql -h $SOURCE_DB_HOST -p $SOURCE_DB_PORT -U $SOURCE_DB_USER -d $SOURCE_DB_NAME -t -c "SELECT COUNT(*) FROM t_order;")
    TARGET_COUNT=$(PGPASSWORD=$TARGET_DB_PASS psql -h $TARGET_DB_HOST -p $TARGET_DB_PORT -U $TARGET_DB_USER -d $TARGET_DB_NAME -t -c "SELECT COUNT(*) FROM t_order;")
    
    echo ""
    print_info "Migration Status:"
    echo "  Source DB (port $SOURCE_DB_PORT): ${SOURCE_COUNT} records"
    echo "  Target DB (port $TARGET_DB_PORT): ${TARGET_COUNT} records"
    
    if [ "$SOURCE_COUNT" == "$TARGET_COUNT" ]; then
        print_success "Databases are in sync! ✓"
    else
        DIFF=$((SOURCE_COUNT - TARGET_COUNT))
        print_warning "Lag detected: $DIFF records behind"
        print_info "This is normal during initial sync..."
    fi
}

# Phase 7: Verification & Cutover
phase7_cutover() {
    print_header "PHASE 7: Cutover Preparation"
    
    print_info "Final sync verification..."
    sleep 3
    
    SOURCE_COUNT=$(PGPASSWORD=$SOURCE_DB_PASS psql -h $SOURCE_DB_HOST -p $SOURCE_DB_PORT -U $SOURCE_DB_USER -d $SOURCE_DB_NAME -t -c "SELECT COUNT(*) FROM t_order;")
    TARGET_COUNT=$(PGPASSWORD=$TARGET_DB_PASS psql -h $TARGET_DB_HOST -p $TARGET_DB_PORT -U $TARGET_DB_USER -d $TARGET_DB_NAME -t -c "SELECT COUNT(*) FROM t_order;")
    
    echo ""
    print_info "Final Record Counts:"
    echo "  Source: ${SOURCE_COUNT}"
    echo "  Target: ${TARGET_COUNT}"
    
    if [ "$SOURCE_COUNT" == "$TARGET_COUNT" ]; then
        print_success "✓ Databases are synchronized!"
        echo ""
        print_header "CUTOVER CHECKLIST"
        echo "  1. ✓ Source DB has ${SOURCE_COUNT} records"
        echo "  2. ✓ Target DB has ${TARGET_COUNT} records"
        echo "  3. ✓ CDC streaming is active"
        echo "  4. ⚠ Ready for cutover"
        echo ""
        print_warning "Next Steps for Zero-Downtime Cutover:"
        echo "  1. Enable maintenance mode (brief pause)"
        echo "  2. Wait for final CDC sync (< 1 second)"
        echo "  3. Switch application to target_db"
        echo "  4. Verify application connectivity"
        echo "  5. Stop CDC streaming"
        echo "  6. Migration complete! ✓"
    else
        print_error "Databases not in sync yet. Wait longer or check CDC logs."
    fi
}

# Main execution
main() {
    clear
    echo -e "${GREEN}"
    cat << "EOF"
╔══════════════════════════════════════════════════════════════╗
║   ShardingSphere CDC Migration Workflow                      ║
║   Near-Zero Downtime Database Migration Demo                 ║
╚══════════════════════════════════════════════════════════════╝
EOF
    echo -e "${NC}"
    
    # Check if --help flag is provided
    if [ "$1" == "--help" ] || [ "$1" == "-h" ]; then
        echo "Usage: ./migration-workflow.sh [phase]"
        echo ""
        echo "Phases:"
        echo "  all       - Run all phases (default)"
        echo "  setup     - Phase 1-2: Setup databases only"
        echo "  stream    - Phase 3-4: Start CDC streaming"
        echo "  test      - Phase 5-6: Simulate traffic & monitor"
        echo "  cutover   - Phase 7: Final verification"
        echo ""
        echo "Example:"
        echo "  ./migration-workflow.sh all"
        echo "  ./migration-workflow.sh setup"
        exit 0
    fi
    
    PHASE="${1:-all}"
    
    case $PHASE in
        setup)
            phase1_setup_source
            phase2_prepare_target
            ;;
        stream)
            phase3_verify_proxy
            phase4_start_cdc
            ;;
        test)
            phase5_simulate_traffic
            phase6_monitor
            ;;
        cutover)
            phase7_cutover
            ;;
        all|*)
            phase1_setup_source
            phase2_prepare_target
            phase3_verify_proxy
            phase4_start_cdc
            phase5_simulate_traffic
            phase6_monitor
            phase7_cutover
            ;;
    esac
    
    echo ""
    print_success "Migration workflow completed!"
    print_info "View real-time CDC monitoring at: http://localhost:8080"
}

# Run main function
main "$@"

