# ShardingSphere CDC (Change Data Capture) Demo with Web UI

This project demonstrates Apache ShardingSphere's **CDC (Change Data Capture)** feature with a beautiful, real-time monitoring web interface. Track database changes as they happen with live statistics, event streaming, and automated data generation.

![CDC Architecture](https://shardingsphere.apache.org/document/current/img/cdc/cdc-architecture.png)

## 🎯 What is CDC?

Change Data Capture (CDC) is a feature that captures and streams database changes (INSERT, UPDATE, DELETE) in real-time. This is different from ShardingSphere's Migration feature:

- **CDC**: Streams changes to external applications for consumption (e.g., event-driven architectures, audit logs, analytics)
- **Migration**: Transfers data from source to target databases for migration purposes

## ✨ Features

- 🔄 **Real-time CDC Streaming**: Capture database changes as they happen
- 📊 **Live Statistics Dashboard**: Monitor events, throughput, and uptime
- 🎨 **Beautiful Web UI**: Modern, responsive interface with real-time updates
- 🔌 **WebSocket Integration**: Instant event notifications via WebSocket
- 🎮 **Data Generator**: Simulate database operations (INSERT, UPDATE, DELETE)
- 📡 **REST API**: Full control via RESTful endpoints
- 📈 **Event History**: View recent CDC events with details

## 🏗️ Architecture

```
PostgreSQL Database (Source)
       ↓
ShardingSphere Proxy (CDC Server on port 33071)
       ↓
CDC Client (Spring Boot App)
       ↓
WebSocket + REST API
       ↓
Web UI (Real-time Dashboard)
```

## 📋 Prerequisites

- **Java 8** or higher
- **Maven 3.2+**
- **Docker and Docker Compose** (or Podman)
- **PostgreSQL 12+** with WAL enabled

## 🚀 Quick Start

### Step 1: Start the Infrastructure

Start PostgreSQL and ZooKeeper containers:

```bash
podman-compose up -d
# or
docker-compose up -d
```

This starts:
- PostgreSQL on port 5432
- ZooKeeper on port 2181

### Step 2: Configure PostgreSQL

Connect to PostgreSQL and set up the source database:

```bash
podman-compose exec postgres psql -U postgres -W
# Password: root
```

Execute the following SQL commands:

```sql
-- Create the source database
DROP DATABASE IF EXISTS migration_ds_0;
CREATE DATABASE migration_ds_0;

\c migration_ds_0

-- Create the sample table
CREATE TABLE t_order (
    order_id INT NOT NULL,
    user_id INT NOT NULL,
    status VARCHAR(45) NULL,
    PRIMARY KEY (order_id)
);

-- Insert initial data
INSERT INTO t_order (order_id, user_id, status) VALUES
(1, 2, 'init'), (2, 4, 'init'), (3, 6, 'init'),
(4, 1, 'init'), (5, 3, 'init'), (6, 5, 'init');
```

**Configure PostgreSQL for CDC** (if not already done):

Edit `postgresql.conf`:
```
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
wal_sender_timeout = 0
max_connections = 600
```

Edit `pg_hba.conf` to allow replication:
```
host replication postgres 0.0.0.0/0 md5
```

Restart PostgreSQL after changes.

Type `\q` to exit psql.

### Step 3: Start ShardingSphere Proxy

The proxy configuration is already set up in `apache-shardingsphere-5.5.2-shardingsphere-proxy-bin/conf/global.yaml` with CDC enabled.

**Key CDC Configuration:**
```yaml
props:
  proxy-frontend-database-protocol-type: PostgreSQL
  sql-show: true
  proxy-default-port: 3308
  cdc-server-port: 33071  # CDC Server port
```

Start the proxy:

```bash
cd apache-shardingsphere-5.5.2-shardingsphere-proxy-bin
export SHARDINGSPHERE_PROXY_PASSWORD=root
./bin/start.sh 3308
```

Check the logs to ensure it started successfully:
```bash
tail -f logs/stdout.log
```

### Step 4: Configure ShardingSphere via DistSQL

Connect to the proxy:

```bash
podman-compose exec postgres psql -h host.containers.internal -p 3308 -U root -d "dbname=postgres sslmode=disable" -W
# Password: root
```

Execute the following DistSQL commands:

```sql
-- Create a logical database
CREATE DATABASE sharding_db;
\c sharding_db

-- Register storage units (you can add multiple for sharding)
REGISTER STORAGE UNIT ds_0 (
    URL='jdbc:postgresql://127.0.0.1:5432/migration_ds_0',
    USER='postgres',
    PASSWORD='root',
    PROPERTIES("minPoolSize"="1","maxPoolSize"="20","idleTimeout"="60000")
);

-- Create sharding rules (optional, for demonstration)
CREATE SHARDING TABLE RULE t_order(
    STORAGE_UNITS(ds_0),
    SHARDING_COLUMN=order_id,
    TYPE(NAME="hash_mod",PROPERTIES("sharding-count"="1")),
    KEY_GENERATE_STRATEGY(COLUMN=order_id,TYPE(NAME="snowflake"))
);

-- Create the table through the proxy
CREATE TABLE t_order (
    order_id INT NOT NULL,
    user_id INT NOT NULL,
    status VARCHAR(45) NULL,
    PRIMARY KEY (order_id)
);
```

Type `\q` to exit.

### Step 5: Build and Run the CDC Application

Build the Spring Boot application:

```bash
mvn clean package
```

Run the application:

```bash
java -jar target/shardingsphere-proxy-cdc-demo-1.0-SNAPSHOT.jar
```

The application will start on **port 8080**.

### Step 6: Open the Web UI

Open your browser and navigate to:

```
http://localhost:8080
```

You should see the beautiful CDC monitoring dashboard! 🎉

## 🎮 Using the Web UI

### CDC Controls

1. **Connect**: Click to connect to the CDC Server (ShardingSphere Proxy)
2. **Start Streaming**: After connecting, start CDC streaming to receive events
3. **Stop**: Stop streaming and disconnect
4. **Reset Stats**: Clear statistics and event history

### Data Generator

Simulate database operations automatically:

1. **Operation Type**: Choose from:
   - **Mixed**: 60% INSERT, 30% UPDATE, 10% DELETE
   - **INSERT Only**: Only insert new records
   - **UPDATE Only**: Only update existing records
   - **DELETE Only**: Only delete records

2. **Interval**: Set the interval in milliseconds between operations (default: 1000ms)

3. **Start Generator**: Begin automatic data generation
4. **Stop Generator**: Stop automatic generation

### Manual Insert

Insert a single order manually:
- Enter User ID
- Enter Status (e.g., "pending", "completed")
- Click "Insert Order"

### Real-time Dashboard

The dashboard shows:
- **Total Events**: Total CDC events received
- **Inserts/Updates/Deletes**: Breakdown by operation type
- **Events/sec**: Throughput rate
- **Uptime**: How long CDC has been running
- **Recent Events**: Live feed of CDC events
- **System Messages**: Status updates and errors

## 📡 REST API Endpoints

All endpoints are available at `http://localhost:8080/api/cdc`:

### CDC Operations

- `POST /api/cdc/connect` - Connect to CDC server
- `POST /api/cdc/start` - Start CDC streaming
- `POST /api/cdc/stop` - Stop CDC streaming
- `GET /api/cdc/status` - Get CDC status
- `GET /api/cdc/statistics` - Get statistics
- `GET /api/cdc/events?limit=50` - Get recent events
- `POST /api/cdc/reset` - Reset statistics

### Data Generator

- `POST /api/cdc/generator/start?interval=1000&operation=MIXED` - Start generator
- `POST /api/cdc/generator/stop` - Stop generator
- `GET /api/cdc/generator/status` - Get generator status
- `POST /api/cdc/generator/insert?userId=1&status=pending` - Manual insert

### Example API Calls

```bash
# Connect to CDC
curl -X POST http://localhost:8080/api/cdc/connect

# Start streaming
curl -X POST http://localhost:8080/api/cdc/start

# Get statistics
curl http://localhost:8080/api/cdc/statistics

# Start data generator
curl -X POST "http://localhost:8080/api/cdc/generator/start?interval=500&operation=MIXED"
```

## 🔌 WebSocket Integration

Connect to `ws://localhost:8080/ws/cdc` to receive real-time updates:

```javascript
const ws = new WebSocket('ws://localhost:8080/ws/cdc');

ws.onmessage = (event) => {
    const message = JSON.parse(event.data);
    
    switch (message.type) {
        case 'event':
            console.log('CDC Event:', message.data);
            break;
        case 'statistics':
            console.log('Statistics:', message.data);
            break;
        case 'status':
            console.log('Status:', message.message);
            break;
        case 'error':
            console.error('Error:', message.message);
            break;
    }
};
```

## ⚙️ Configuration

Configuration is in `src/main/resources/application.properties`:

```properties
# Server Configuration
server.port=8080

# CDC Server Configuration
cdc.server.host=127.0.0.1
cdc.server.port=33071
cdc.server.timeout=10000

# CDC Database Configuration
cdc.database.name=sharding_db
cdc.table.name=t_order

# CDC Login Configuration
cdc.login.username=root
cdc.login.password=root

# CDC Options
cdc.full.sync=true

# Data Source Configuration (for data generator)
datasource.url=jdbc:postgresql://localhost:5432/migration_ds_0
datasource.username=postgres
datasource.password=root
```

## 📚 Project Structure

```
src/main/java/org/apache/shardingsphere/example/proxy/cdc/
├── CDCApplication.java              # Spring Boot main class
├── config/
│   └── WebSocketConfig.java         # WebSocket configuration
├── controller/
│   └── CDCController.java           # REST API endpoints
├── model/
│   ├── CDCEvent.java                # CDC event model
│   ├── CDCStatistics.java           # Statistics model
│   └── CDCStatus.java               # Status model
├── service/
│   ├── CDCClientService.java        # CDC client logic
│   ├── DataGeneratorService.java    # Data generator
│   └── WebSocketService.java        # WebSocket broadcasting
└── websocket/
    └── CDCWebSocketHandler.java     # WebSocket handler

src/main/resources/
├── application.properties           # Application configuration
└── static/
    ├── index.html                   # Main web UI
    ├── css/
    │   └── style.css               # Styles
    └── js/
        └── app.js                  # Frontend logic
```

## 🔍 Monitoring CDC in Proxy

You can also monitor CDC tasks via DistSQL in the proxy:

```sql
-- Connect to the proxy
psql -h localhost -p 3308 -U root -d sharding_db

-- View CDC streaming list
SHOW STREAMING LIST;

-- View streaming status (replace with your streaming ID)
SHOW STREAMING STATUS 'j0302p000...';

-- Drop CDC task (when no subscriptions)
DROP STREAMING 'j0302p000...';
```

## 🐛 Troubleshooting

### CDC Client can't connect to CDC Server

- Ensure ShardingSphere Proxy is running
- Check that `cdc-server-port: 33071` is in `global.yaml`
- Verify firewall settings
- Check logs: `apache-shardingsphere-5.5.2-shardingsphere-proxy-bin/logs/stdout.log`

### No events appearing

- Ensure CDC streaming is started (click "Start Streaming")
- Verify the table exists in the logical database
- Check WebSocket connection status
- Start the data generator to create events

### PostgreSQL connection errors

- Verify PostgreSQL is running: `podman ps` or `docker ps`
- Check credentials in `application.properties`
- Ensure database `migration_ds_0` exists
- Verify WAL settings in PostgreSQL

### WebSocket keeps reconnecting

- Check if the Spring Boot app is running
- Verify no firewall blocking port 8080
- Check browser console for errors

## 📖 References

- [ShardingSphere CDC Documentation](https://shardingsphere.apache.org/document/current/en/user-manual/shardingsphere-proxy/cdc/usage/)
- [ShardingSphere CDC Build Guide](https://shardingsphere.apache.org/document/current/en/user-manual/shardingsphere-proxy/cdc/build/)
- [Apache ShardingSphere Official Site](https://shardingsphere.apache.org/)

## 🎉 Success!

You now have a fully functional CDC system with:
- ✅ Real-time change data capture
- ✅ Beautiful web monitoring dashboard
- ✅ Automated data generation
- ✅ Live statistics and metrics
- ✅ WebSocket real-time updates
- ✅ REST API for automation

Enjoy exploring ShardingSphere's powerful CDC capabilities! 🚀
