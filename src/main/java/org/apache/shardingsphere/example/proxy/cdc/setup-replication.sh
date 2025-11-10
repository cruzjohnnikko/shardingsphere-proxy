#!/bin/bash
set -e

echo "Setting up PostgreSQL Streaming Replication..."

# Step 1: Configure primary to allow replication with trust auth
echo "1. Configuring primary database for replication..."
podman exec postgres_cdc_demo psql -U postgres -c "CREATE USER replicator WITH REPLICATION;" 2>/dev/null || echo "Replicator user already exists"

podman exec postgres_cdc_demo bash -c "echo 'host replication replicator 0.0.0.0/0 trust' >> /var/lib/postgresql/data/pg_hba.conf"
podman exec postgres_cdc_demo psql -U postgres -c "SELECT pg_reload_conf();"

# Step 2: Clean backup directory on primary
echo "2. Preparing for base backup..."
podman exec postgres_cdc_demo bash -c "rm -rf /tmp/backup"

# Step 3: Create base backup from primary
echo "3. Creating base backup from primary..."
podman exec postgres_cdc_demo bash -c "pg_basebackup -h localhost -U replicator -D /tmp/backup -P -R -X stream -c fast"

# Step 4: Stop replica and copy backup
echo "4. Stopping replica temporarily..."
podman stop postgres_replica_demo
sleep 2

echo "5. Copying backup to replica..."
mkdir -p ~/pg_backup_tmp
podman cp postgres_cdc_demo:/tmp/backup/. ~/pg_backup_tmp/

echo "6. Starting replica to prep directories..."
podman start postgres_replica_demo
sleep 4

echo "7. Cleaning and copying data..."
podman exec postgres_replica_demo bash -c "rm -rf /var/lib/postgresql/data/pgdata && mkdir -p /var/lib/postgresql/data/pgdata"
podman stop postgres_replica_demo
sleep 2

podman cp ~/pg_backup_tmp/. postgres_replica_demo:/var/lib/postgresql/data/pgdata/
rm -rf ~/pg_backup_tmp

echo "8. Starting replica to set ownership..."
podman start postgres_replica_demo
sleep 4

echo "9. Setting ownership and creating standby signal..."
podman exec postgres_replica_demo bash -c "chown -R postgres:postgres /var/lib/postgresql/data/pgdata && touch /var/lib/postgresql/data/pgdata/standby.signal"

echo "10. Restarting replica..."
podman restart postgres_replica_demo

sleep 8

# Step 7: Verify replication
echo "7. Verifying replication status..."
echo ""
echo "Primary replication status:"
podman exec postgres_cdc_demo psql -U postgres -c "SELECT client_addr, state, sync_state FROM pg_stat_replication;" || echo "Checking..."

sleep 2

echo ""
echo "Replica recovery status (should return 't' for true):"
podman exec postgres_replica_demo psql -U postgres -c "SELECT pg_is_in_recovery();" || echo "Replica still starting..."

echo ""
echo "✅ PostgreSQL Streaming Replication setup complete!"
echo ""
echo "Test it:"
echo "1. Insert data into primary: podman exec postgres_cdc_demo psql -U postgres -c \"INSERT INTO t_order VALUES (99999, 999, 'test-replication');\""
echo "2. Check replica: podman exec postgres_replica_demo psql -U postgres -c \"SELECT * FROM t_order WHERE order_id = 99999;\""

