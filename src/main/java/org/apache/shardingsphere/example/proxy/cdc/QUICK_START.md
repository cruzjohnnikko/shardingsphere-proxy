# 🚀 Quick Start Guide - ShardingSphere CDC

## One-Command Setup (After Prerequisites)

```bash
# 1. Start containers
podman-compose up -d

# 2. Setup database (in another terminal)
podman-compose exec postgres psql -U postgres -c "CREATE DATABASE migration_ds_0;" -c "\c migration_ds_0" -c "CREATE TABLE t_order (order_id INT PRIMARY KEY, user_id INT, status VARCHAR(45));"

# 3. Start Proxy
cd apache-shardingsphere-5.5.2-shardingsphere-proxy-bin && ./bin/start.sh 3308 &

# 4. Build and run CDC app
mvn clean package && java -jar target/shardingsphere-proxy-cdc-demo-1.0-SNAPSHOT.jar
```

## Access the UI

Open browser: **http://localhost:8080**

## Quick Test

1. Click **"Connect"** button
2. Click **"Start Streaming"** button  
3. Click **"Start Generator"** button
4. Watch events flow in real-time! 🎉

## Key Ports

- **8080**: Web UI & REST API
- **3308**: ShardingSphere Proxy (SQL)
- **33071**: CDC Server
- **5432**: PostgreSQL
- **2181**: ZooKeeper

## Common Commands

```bash
# Check if proxy is running
ps aux | grep shardingsphere

# View proxy logs
tail -f apache-shardingsphere-5.5.2-shardingsphere-proxy-bin/logs/stdout.log

# Stop proxy
cd apache-shardingsphere-5.5.2-shardingsphere-proxy-bin && ./bin/stop.sh

# Connect to proxy
psql -h localhost -p 3308 -U root -d sharding_db

# View source database
psql -h localhost -p 5432 -U postgres -d migration_ds_0
```

## REST API Examples

```bash
# Connect to CDC
curl -X POST http://localhost:8080/api/cdc/connect

# Start streaming
curl -X POST http://localhost:8080/api/cdc/start

# Get status
curl http://localhost:8080/api/cdc/status

# Get statistics
curl http://localhost:8080/api/cdc/statistics

# Start generator (1 operation per second, mixed operations)
curl -X POST "http://localhost:8080/api/cdc/generator/start?interval=1000&operation=MIXED"

# Stop generator
curl -X POST http://localhost:8080/api/cdc/generator/stop

# Manual insert
curl -X POST "http://localhost:8080/api/cdc/generator/insert?userId=123&status=pending"

# Stop CDC
curl -X POST http://localhost:8080/api/cdc/stop

# Reset statistics
curl -X POST http://localhost:8080/api/cdc/reset
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Can't connect to CDC | Check if proxy is running with CDC port 33071 configured |
| No events appearing | Verify streaming is started and generator is running |
| WebSocket disconnected | Check if Spring Boot app is running on port 8080 |
| Database connection error | Verify PostgreSQL is running and credentials are correct |

## Architecture in 30 seconds

```
PostgreSQL → ShardingSphere Proxy (CDC Server) → CDC Client (Your App) → Web UI
```

1. **PostgreSQL** writes data
2. **Proxy** captures changes via WAL
3. **CDC Client** receives events
4. **Web UI** displays in real-time

That's it! 🎉

