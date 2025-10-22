# ShardingSphere-Proxy CDC Demo: Near-Zero Downtime Database Migration

This demo showcases how to use Apache ShardingSphere-Proxy's Change Data Capture (CDC) feature for database migration with near-zero downtime.

## How it Works

The core idea is to use ShardingSphere-Proxy to manage the migration from a source database to a set of sharded target databases.

1.  **Initial Data Load:** ShardingSphere-Proxy first performs a full data migration from the source database to the target databases.
2.  **Change Data Capture (CDC):** Concurrently, ShardingSphere's CDC feature captures any data changes (inserts, updates, deletes) from the source database's transaction log (e.g., PostgreSQL's WAL).
3.  **Applying Changes:** These captured changes are then streamed and applied to the target databases in near real-time.

This process ensures that the target databases are always in sync with the source, allowing for a seamless switchover with minimal downtime.

## Prerequisites

*   Java 8 or higher
*   Maven 3.2+
*   Docker and Docker Compose

---

## Running the Demo

### Step 1: Start the Environment

This command will start PostgreSQL and ZooKeeper containers. We also create a Docker volume (`postgres_data`) to ensure your database data persists even if the container is removed and recreated.

```bash
podman-compose up -d
```

### Step 2: Configure the Source Database

Connect to the PostgreSQL container to create the source and target databases.

```bash
# This command executes 'psql' inside the 'postgres' container
podman-compose exec postgres psql -U postgres -W
# A prompt for the password will appear. The password is 'root'.
```

Once connected (you'll see the `postgres=#` prompt), execute the following SQL commands:

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

-- Create target databases
DROP DATABASE IF EXISTS migration_ds_10;
CREATE DATABASE migration_ds_10;
DROP DATABASE IF EXISTS migration_ds_11;
CREATE DATABASE migration_ds_11;
DROP DATABASE IF EXISTS migration_ds_12;
CREATE DATABASE migration_ds_12;
```

Type `\q` to exit `psql`.

### Step 3: Configure and Start ShardingSphere-Proxy

The ShardingSphere-Proxy is a Java application that you must run on your host machine.

1.  **Download ShardingSphere-Proxy** from the [official website](https://shardingsphere.apache.org/document/current/en/downloads/).
2.  **Extract the archive.**
3.  **Configure `global.yaml`**: Newer versions of the proxy use `conf/global.yaml`. Replace its contents with the configuration below. This sets up cluster mode with ZooKeeper, creates a `root` user, and sets the proxy to use the PostgreSQL protocol.

    ```yaml
    # Save this content to a file named `global.yaml` in the `conf` directory
    # of your ShardingSphere-Proxy installation, replacing the existing file.
    
    mode:
      type: Cluster
      repository:
        type: ZooKeeper
        props:
          namespace: governance_ds
          server-lists: localhost:2181

    authority:
      users:
        - user: root@%
          password: root
      privilege:
        type: ALL_PERMITTED

    props:
      proxy-frontend-database-protocol-type: PostgreSQL
    ```

4.  **Add PostgreSQL JDBC Driver**: Download the PostgreSQL JDBC driver JAR from [here](https://jdbc.postgresql.org/download/) and place it in the `ext-lib/` directory of your ShardingSphere-Proxy installation.
5.  **Start ShardingSphere-Proxy**: Open a **new terminal window**, navigate to your ShardingSphere-Proxy directory, and run:

    ```bash
    export SHARDINGSPHERE_PROXY_PASSWORD=root && sh apache-shardingsphere-5.5.2-shardingsphere-proxy-bin/bin/start.sh 3308
    ```
    Leave this terminal running. You can check `logs/stdout.log` to confirm it started successfully.

### Step 4: Configure the Migration via DistSQL

Now, connect to the running ShardingSphere-Proxy instance on port `3308`.

```bash
# This command connects to the proxy from within the postgres container
    podman-compose exec postgres psql -h host.containers.internal -p 3308 -U root -d "dbname=postgres sslmode=disable" -W
# A prompt for the password will appear. The password is 'root'.
```

**Important:** You are now connected to the proxy, not the database. The prompt will look similar, but these commands are ShardingSphere's DistSQL.

Execute the following commands to set up the migration:

```sql
-- Create a logical database
CREATE DATABASE sharding_db;
\c sharding_db


REGISTER STORAGE UNIT migration_ds_10 (URL='jdbc:postgresql://127.0.0.1:5432/migration_ds_10', USER='postgres', PASSWORD='root', PROPERTIES("minPoolSize"="1","maxPoolSize"="20","idleTimeout"="60000")), migration_ds_11 (URL='jdbc:postgresql://127.0.0.1:5432/migration_ds_11', USER='postgres', PASSWORD='root', PROPERTIES("minPoolSize"="1","maxPoolSize"="20","idleTimeout"="60000")), migration_ds_12 (URL='jdbc:postgresql://127.0.0.1:5432/migration_ds_12', USER='postgres', PASSWORD='root', PROPERTIES("minPoolSize"="1","maxPoolSize"="20","idleTimeout"="60000"));


CREATE TABLE t_order (order_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (order_id));

-- Register the source database for migration
REGISTER MIGRATION SOURCE STORAGE UNIT ds_0 (
    URL='jdbc:postgresql://127.0.0.1:5432/migration_ds_0',
    USER='postgres',
    PASSWORD='root'
);

-- Start the migration
MIGRATE TABLE ds_0.t_order INTO t_order;
```

### Step 5: Start the Data Simulation App

In another new terminal window, navigate back to the project directory and run the Java application. This will continuously insert new data into the *source* database (`migration_ds_0`).

```bash
mvn clean package
export CDC_DB_USER=postgres
export CDC_DB_PASSWORD=root
# Then run your Java application
java -jar target/shardingsphere-proxy-cdc-demo-1.0-SNAPSHOT-jar-with-dependencies.jar
```

### Step 6: Monitor and Verify

You can check the migration status in the `psql` session connected to the proxy:

```sql
-- See all migration jobs
SHOW MIGRATION LIST;

-- Get details for a specific job (replace <job_id> with the actual ID)
SHOW MIGRATION STATUS '<job_id>'; 
```

While the Java app is running, query the sharded table through the proxy. You will see the count increase as new data is migrated in near real-time.

```sql
SELECT COUNT(*) FROM t_order;
```
```