# Snowflake Receiver

[![Project Status: Alpha](https://img.shields.io/badge/status-alpha-orange.svg)](https://github.com/npcomplete777/snowflakereceiver)
[![Go Version](https://img.shields.io/badge/go-1.24+-blue.svg)](https://go.dev/)
[![OpenTelemetry](https://img.shields.io/badge/OpenTelemetry-0.135.0-blueviolet.svg)](https://opentelemetry.io/)

This custom Snowflake receiver is an OpenTelemetry Collector component that collects metrics from Snowflake data warehouses. It provides comprehensive observability across query performance, warehouse utilization, credit consumption, storage, security events, and data pipeline operations.

## Table of Contents

- [Features](#features)
- [Supported Metrics](#supported-metrics)
- [Data Sources](#data-sources)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Architecture](#architecture)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

---

## Features

### Core Capabilities

- **300+ Metrics Across 11 Categories** - Comprehensive monitoring of Snowflake operations
- **Real-Time & Historical Data** - Seconds-level latency from INFORMATION_SCHEMA, 45min-3hr from ACCOUNT_USAGE
- **Event Tables Support** - Near real-time monitoring (seconds) for operational events
- **Organization-Level Metrics** - Multi-account monitoring for Enterprise deployments
- **Custom Query Executor** - Define your own metrics with arbitrary SQL queries

### Production-Ready Features

#### Cardinality Protection (Circuit Breakers)
Prevents metric explosion from high-cardinality dimensions:
- Configurable limits per dimension (users, schemas, databases, roles)
- Automatic fallback to aggregated dimensions when limits exceeded
- Resets every 24 hours
- Self-monitoring of dropped dimension values

**Example:** With `max_users_cardinality: 500`, the first 500 users are tracked individually. Users 501+ are aggregated as `high_cardinality_user`, preventing backend overload while maintaining useful metrics.

#### Rate Limiting
Protects Snowflake from query overload:
- Configurable queries-per-second (QPS) limit
- Token bucket algorithm for smooth rate distribution
- Prevents Snowflake throttling
- Recommended: 10 QPS for production, 5 QPS for trial accounts

#### Fine-Grained Polling Control
Independent polling intervals per metric category:
- Real-time metrics: 1 minute (operational monitoring)
- Historical metrics: 5 minutes (trend analysis)
- Storage metrics: 30 minutes (daily updates)
- Optimize Snowflake credit consumption vs. data freshness

#### Retry Logic with Exponential Backoff
Resilient error handling:
- Configurable retry attempts (default: 3)
- Exponential backoff with jitter
- Prevents cascading failures
- Detailed error logging for troubleshooting

---

## Supported Metrics

### Metric Categories

The receiver collects **300+ metrics** across 11 categories:

| Category | Interval | Latency | Metrics | Source |
|----------|----------|---------|---------|--------|
| **Current Queries** | 1m | Seconds | 35+ | INFORMATION_SCHEMA |
| **Warehouse Load** | 1m | Seconds | 25+ | INFORMATION_SCHEMA |
| **Query History** | 5m | 45min-3hr | 60+ | ACCOUNT_USAGE |
| **Credit Usage** | 5m | 45min-3hr | 20+ | ACCOUNT_USAGE |
| **Login History** | 10m | 45min-3hr | 15+ | ACCOUNT_USAGE |
| **Storage Metrics** | 30m | Daily | 10+ | ACCOUNT_USAGE |
| **Database Storage** | 30m | Daily | 8+ | ACCOUNT_USAGE |
| **Data Pipeline** | 10m | 45min-3hr | 15+ | ACCOUNT_USAGE |
| **Task History** | 10m | 45min-3hr | 20+ | ACCOUNT_USAGE |
| **Replication Usage** | 15m | 45min-3hr | 10+ | ACCOUNT_USAGE |
| **Auto-Clustering** | 15m | 45min-3hr | 12+ | ACCOUNT_USAGE |

### Sample Metrics

#### Query Performance
- `snowflake.query.count` - Number of queries by type, status, warehouse
- `snowflake.query.execution_time` - Average query execution time
- `snowflake.query.bytes_scanned` - Data scanned per query
- `snowflake.query.compilation_time` - Query compilation overhead
- `snowflake.query.blocked` - Queries blocked by locks

**Dimensions:** `warehouse_name`, `database_name`, `schema_name`, `user_name`, `role_name`, `query_type`, `execution_status`, `error_code`

#### Warehouse Utilization
- `snowflake.warehouse.queries_running` - Currently executing queries
- `snowflake.warehouse.queries_queued_load` - Queued due to warehouse load
- `snowflake.warehouse.queries_queued_provisioning` - Queued for warehouse startup
- `snowflake.warehouse.queries_blocked` - Blocked by concurrency limits

**Dimensions:** `warehouse_name`

#### Credit Consumption
- `snowflake.credits.total` - Total credits consumed
- `snowflake.credits.compute` - Compute credits
- `snowflake.credits.cloud_services` - Cloud services credits

**Dimensions:** `warehouse_name`

#### Storage
- `snowflake.storage.total_bytes` - Total storage consumed
- `snowflake.storage.stage_bytes` - Stage storage
- `snowflake.storage.failsafe_bytes` - Fail-safe storage
- `snowflake.database.storage_bytes` - Per-database storage

**Dimensions:** `database_name`

#### Security & Access
- `snowflake.login.count` - Login attempts by status
- `snowflake.login.success_rate` - Percentage of successful logins

**Dimensions:** `user_name`, `client_type`, `error_message`

#### Data Pipeline
- `snowflake.pipe.credits` - Snowpipe credit consumption
- `snowflake.pipe.bytes_inserted` - Data ingested via Snowpipe
- `snowflake.pipe.files_inserted` - Files processed

**Dimensions:** `pipe_name`

#### Task Execution
- `snowflake.task.count` - Task execution count by state
- `snowflake.task.execution_time` - Task runtime

**Dimensions:** `database_name`, `schema_name`, `task_name`, `state`

### Self-Monitoring Metrics

The receiver emits its own health metrics:

- `snowflake.receiver.queries.total` - Total queries executed
- `snowflake.receiver.errors.total` - Total errors encountered
- `snowflake.receiver.retries.total` - Total retry attempts
- `snowflake.receiver.success.rate` - Query success percentage
- `snowflake.receiver.cardinality.dropped_users` - Users aggregated due to cardinality limits
- `snowflake.receiver.cardinality.dropped_schemas` - Schemas aggregated
- `snowflake.receiver.cardinality.dropped_databases` - Databases aggregated

---

## Data Sources

The receiver queries multiple Snowflake schemas to provide comprehensive monitoring:

### INFORMATION_SCHEMA (Real-Time - Seconds Latency)

These table functions provide near real-time operational data:

- **`QUERY_HISTORY()`** - Currently executing and recently completed queries
  - Columns: QUERY_ID, QUERY_TEXT, USER_NAME, WAREHOUSE_NAME, WAREHOUSE_SIZE, DATABASE_NAME, SCHEMA_NAME, QUERY_TYPE, EXECUTION_STATUS, START_TIME, END_TIME, TOTAL_ELAPSED_TIME, BYTES_SCANNED, ROWS_PRODUCED, ERROR_CODE, ERROR_MESSAGE
  - Latency: Seconds
  - Use case: Real-time operational monitoring, live dashboards

- **`WAREHOUSE_LOAD_HISTORY()`** - Warehouse queue depth and utilization
  - Columns: WAREHOUSE_NAME, START_TIME, END_TIME, AVG_RUNNING, AVG_QUEUED_LOAD, AVG_QUEUED_PROVISIONING, AVG_BLOCKED
  - Latency: Seconds
  - Use case: Capacity planning, auto-scaling decisions

### ACCOUNT_USAGE (Historical - 45min-3hr Latency)

These tables provide comprehensive historical analytics:

- **`QUERY_HISTORY`** - Historical query execution data
  - Additional columns vs INFORMATION_SCHEMA: Credits used, spilling metrics, data transfer stats
  - Latency: 45 minutes to 3 hours
  - Use case: Historical analysis, cost attribution, performance trending

- **`WAREHOUSE_METERING_HISTORY`** - Credit consumption by warehouse
  - Columns: WAREHOUSE_NAME, START_TIME, END_TIME, CREDITS_USED, CREDITS_USED_COMPUTE, CREDITS_USED_CLOUD_SERVICES
  - Latency: 45 minutes to 3 hours
  - Use case: Cost tracking, budget monitoring

- **`STORAGE_USAGE`** - Account-level storage consumption
  - Columns: USAGE_DATE, STORAGE_BYTES, STAGE_BYTES, FAILSAFE_BYTES
  - Latency: Updates daily
  - Use case: Storage trending, capacity planning

- **`LOGIN_HISTORY`** - Authentication events
  - Columns: EVENT_ID, EVENT_TIMESTAMP, USER_NAME, CLIENT_TYPE, IS_SUCCESS, ERROR_CODE, ERROR_MESSAGE
  - Latency: 45 minutes to 2 hours
  - Use case: Security monitoring, compliance auditing

- **`PIPE_USAGE_HISTORY`** - Snowpipe data ingestion
  - Columns: PIPE_NAME, START_TIME, END_TIME, CREDITS_USED, BYTES_INSERTED, FILES_INSERTED
  - Latency: 45 minutes to 3 hours
  - Use case: ETL monitoring, pipeline health

- **`DATABASE_STORAGE_USAGE_HISTORY`** - Per-database storage
  - Columns: USAGE_DATE, DATABASE_NAME, STORAGE_BYTES
  - Latency: Updates daily
  - Use case: Database-level cost attribution

- **`TASK_HISTORY`** - Scheduled task execution
  - Columns: TASK_ID, DATABASE_NAME, SCHEMA_NAME, TASK_NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME
  - Latency: 45 minutes to 3 hours
  - Use case: Task monitoring, dependency tracking

- **`REPLICATION_USAGE_HISTORY`** - Database replication metrics
  - Columns: DATABASE_NAME, START_TIME, END_TIME, CREDITS_USED, BYTES_TRANSFERRED
  - Latency: 45 minutes to 3 hours
  - Use case: DR monitoring, replication cost tracking

- **`AUTOMATIC_CLUSTERING_HISTORY`** - Auto-clustering operations
  - Columns: START_TIME, END_TIME, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, CREDITS_USED, BYTES_RECLUSTERED, ROWS_RECLUSTERED
  - Latency: 45 minutes to 3 hours
  - Use case: Clustering cost tracking, table maintenance

### EVENT_TABLE (Real-Time - Seconds Latency) [Optional]

Event Tables provide near real-time telemetry with OpenTelemetry trace context:

- **Query Logs** - Query execution events with trace correlation
- **Task Logs** - Task execution events with dependencies
- **Function Logs** - UDF/stored procedure execution
- **Procedure Logs** - Stored procedure telemetry

Latency: Seconds (vs. 45min-3hr for ACCOUNT_USAGE)

**Setup required:**
```sql
CREATE EVENT TABLE SNOWFLAKE.PUBLIC.MONITORING_EVENT_TABLE;
ALTER ACCOUNT SET EVENT_TABLE = 'SNOWFLAKE.PUBLIC.MONITORING_EVENT_TABLE';
```

### ORGANIZATION_USAGE (Multi-Account) [Enterprise Only]

Organization-level metrics across all accounts:

- **`USAGE_IN_CURRENCY_DAILY`** - Org-wide credit consumption
- **`STORAGE_DAILY_HISTORY`** - Org-wide storage
- **`DATA_TRANSFER_HISTORY`** - Cross-account data transfers
- **`CONTRACT_ITEMS`** - Contract and capacity tracking

**Requirements:** Enterprise edition + ORGADMIN role

---

## Prerequisites

### Snowflake Requirements

- **Snowflake Account** - Any edition (Standard, Enterprise, Business Critical)
- **Database Access** - Read access to `SNOWFLAKE` database
- **Schema Access** - Read access to `ACCOUNT_USAGE` schema
- **User Permissions** - Service account with appropriate role

**Recommended Role Setup:**
```sql
-- Create monitoring role
CREATE ROLE SNOWFLAKE_MONITORING;

-- Grant database and schema access
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE SNOWFLAKE_MONITORING;

-- Grant warehouse usage (for query execution)
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE SNOWFLAKE_MONITORING;

-- Create service account
CREATE USER snowflake_monitor 
  PASSWORD='STRONG_PASSWORD'
  DEFAULT_ROLE=SNOWFLAKE_MONITORING
  DEFAULT_WAREHOUSE=COMPUTE_WH;

-- Assign role
GRANT ROLE SNOWFLAKE_MONITORING TO USER snowflake_monitor;
```

**For Organization Metrics (Enterprise):**
```sql
GRANT ROLE ORGADMIN TO USER snowflake_monitor;
```

### OpenTelemetry Collector

- **OpenTelemetry Collector Builder (ocb)** - v0.135.0 or later
- **Go** - Version 1.24 or later
- **Operating System** - Linux, macOS, or Windows

---

## Installation

### Option 1: Build from Source (Recommended)

#### Step 1: Install Prerequisites

```bash
# Install Go 1.24+
# https://go.dev/doc/install

# Install OpenTelemetry Collector Builder
go install go.opentelemetry.io/collector/cmd/builder@v0.135.0
```

#### Step 2: Clone the Repository

```bash
git clone https://github.com/npcomplete777/snowflakereceiver.git
cd snowflakereceiver
```

#### Step 3: Build the Collector

Create `builder-config.yaml`:

```yaml
dist:
  name: otelcol-snowflake
  description: OpenTelemetry Collector with Snowflake receiver
  output_path: ./otelcol-snowflake
  otelcol_version: 0.135.0

receivers:
  - gomod: github.com/npcomplete777/snowflakereceiver/snowflakereceiver v0.0.0
    path: ./snowflakereceiver

exporters:
  - gomod: go.opentelemetry.io/collector/exporter/debugexporter v0.135.0
  - gomod: go.opentelemetry.io/collector/exporter/otlphttpexporter v0.135.0

processors:
  - gomod: go.opentelemetry.io/collector/processor/batchprocessor v0.135.0

extensions:
  - gomod: go.opentelemetry.io/collector/extension/zpagesextension v0.135.0
```

Build:

```bash
builder --config builder-config.yaml
```

The compiled binary will be in `./otelcol-snowflake/otelcol-snowflake`.

#### Step 4: Verify Installation

```bash
./otelcol-snowflake/otelcol-snowflake --version
```

### Option 2: Pre-Built Binary

Coming soon: Pre-built binaries for Linux, macOS, and Windows.

---

## Configuration

### Minimal Configuration

The simplest configuration to get started:

```yaml
receivers:
  snowflake:
    # Snowflake connection
    user: "YOUR_USERNAME"
    password: "YOUR_PASSWORD"
    account: "your-account"
    warehouse: "COMPUTE_WH"
    database: "SNOWFLAKE"
    schema: "ACCOUNT_USAGE"
    
    # Enable basic metrics
    metrics:
      current_queries:
        enabled: true
        interval: "1m"
      
      warehouse_load:
        enabled: true
        interval: "1m"
      
      query_history:
        enabled: true
        interval: "5m"
      
      credit_usage:
        enabled: true
        interval: "5m"

processors:
  batch:
    timeout: 10s

exporters:
  debug:
    verbosity: detailed

service:
  pipelines:
    metrics:
      receivers: [snowflake]
      processors: [batch]
      exporters: [debug]
```

### Production Configuration

Comprehensive configuration with all features:

```yaml
receivers:
  snowflake:
    # ========================================================================
    # CONNECTION SETTINGS
    # ========================================================================
    user: "MONITORING_SERVICE_ACCOUNT"
    password: "${env:SNOWFLAKE_PASSWORD}"  # Use environment variable
    account: "your-prod-account"
    warehouse: "MONITORING_WH"  # Dedicated XS warehouse recommended
    database: "SNOWFLAKE"
    schema: "ACCOUNT_USAGE"
    
    # ========================================================================
    # CARDINALITY PROTECTION (Circuit Breakers)
    # Adjust based on environment size:
    # - Small (<100 users): Use defaults
    # - Medium (100-1000 users): Increase by 2-3x
    # - Large (1000+ users): Increase by 5-10x
    # ========================================================================
    max_users_cardinality: 1000      # Default: 500
    max_schemas_cardinality: 500     # Default: 200
    max_databases_cardinality: 200   # Default: 100
    max_roles_cardinality: 500       # Default: 200
    
    # ========================================================================
    # PERFORMANCE & RELIABILITY
    # ========================================================================
    query_timeout: "60s"             # Timeout per query (default: 30s)
    max_rows_per_query: 50000        # Max rows per query (default: 10000)
    rate_limit_qps: 15               # Queries per second (default: 10)
    max_retries: 3                   # Retry attempts (default: 3)
    retry_initial_delay: "2s"        # Initial retry delay (default: 1s)
    retry_max_delay: "60s"           # Max retry delay (default: 30s)
    
    # ========================================================================
    # METRICS CONFIGURATION
    # ========================================================================
    metrics:
      # Real-time operational metrics (INFORMATION_SCHEMA)
      current_queries:
        enabled: true
        interval: "1m"  # Poll every 1 minute
      
      warehouse_load:
        enabled: true
        interval: "1m"  # Poll every 1 minute
      
      # Historical analytics (ACCOUNT_USAGE)
      query_history:
        enabled: true
        interval: "5m"  # Poll every 5 minutes
      
      credit_usage:
        enabled: true
        interval: "5m"  # Poll every 5 minutes
      
      login_history:
        enabled: true
        interval: "10m"  # Poll every 10 minutes
      
      storage_metrics:
        enabled: true
        interval: "30m"  # Poll every 30 minutes (updates daily)
      
      database_storage:
        enabled: true
        interval: "30m"  # Poll every 30 minutes (updates daily)
      
      data_pipeline:
        enabled: true
        interval: "10m"  # Poll every 10 minutes
      
      task_history:
        enabled: true
        interval: "10m"  # Poll every 10 minutes
      
      replication_usage:
        enabled: true
        interval: "15m"  # Poll every 15 minutes
      
      auto_clustering_history:
        enabled: true
        interval: "15m"  # Poll every 15 minutes
    
    # ========================================================================
    # EVENT TABLES (Seconds-Level Latency) - Optional
    # Requires Event Table setup in Snowflake
    # ========================================================================
    event_tables:
      enabled: true
      table_name: "SNOWFLAKE.PUBLIC.MONITORING_EVENT_TABLE"
      
      query_logs:
        enabled: true
        interval: "30s"  # Poll every 30 seconds
      
      task_logs:
        enabled: true
        interval: "30s"  # Poll every 30 seconds
    
    # ========================================================================
    # ORGANIZATION METRICS (Multi-Account) - Enterprise Only
    # Requires ORGADMIN role
    # ========================================================================
    organization:
      enabled: true
      
      org_credit_usage:
        enabled: true
        interval: "1h"  # Poll every hour
      
      org_storage_usage:
        enabled: true
        interval: "1h"  # Poll every hour
      
      org_data_transfer:
        enabled: true
        interval: "1h"  # Poll every hour
      
      org_contract_usage:
        enabled: true
        interval: "12h"  # Poll twice daily
    
    # ========================================================================
    # CUSTOM QUERIES - Define Your Own Metrics
    # ========================================================================
    custom_queries:
      enabled: true
      queries:
        # Example: Query cost by user
        - name: "query_cost_by_user"
          interval: "5m"
          metric_type: "gauge"
          value_column: "TOTAL_CREDITS"
          label_columns: ["USER_NAME", "WAREHOUSE_NAME"]
          sql: |
            SELECT 
              USER_NAME,
              WAREHOUSE_NAME,
              SUM(CREDITS_USED_CLOUD_SERVICES) as TOTAL_CREDITS
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE START_TIME >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
              AND CREDITS_USED_CLOUD_SERVICES > 0
            GROUP BY USER_NAME, WAREHOUSE_NAME
            LIMIT 100

processors:
  batch:
    timeout: 10s
    send_batch_size: 5000

exporters:
  otlphttp:
    endpoint: "https://your-backend.example.com/v1/metrics"
    headers:
      api-key: "${env:OTLP_API_KEY}"

service:
  pipelines:
    metrics:
      receivers: [snowflake]
      processors: [batch]
      exporters: [otlphttp]
```

### Configuration Reference

#### Connection Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `user` | string | **required** | Snowflake username |
| `password` | string | **required** | Snowflake password (use env vars) |
| `account` | string | **required** | Snowflake account identifier |
| `warehouse` | string | **required** | Warehouse for query execution |
| `database` | string | `SNOWFLAKE` | Database name |
| `schema` | string | `ACCOUNT_USAGE` | Schema name |

#### Cardinality Protection

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_users_cardinality` | int | `500` | Max unique users before aggregation |
| `max_schemas_cardinality` | int | `200` | Max unique schemas before aggregation |
| `max_databases_cardinality` | int | `100` | Max unique databases before aggregation |
| `max_roles_cardinality` | int | `200` | Max unique roles before aggregation |

When limits are exceeded, new dimension values are aggregated as `high_cardinality_<dimension>`.

#### Performance Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `query_timeout` | duration | `30s` | Timeout for each Snowflake query |
| `max_rows_per_query` | int | `10000` | Maximum rows returned per query |
| `rate_limit_qps` | int | `10` | Queries per second limit |
| `max_retries` | int | `3` | Number of retry attempts |
| `retry_initial_delay` | duration | `1s` | Initial retry delay |
| `retry_max_delay` | duration | `30s` | Maximum retry delay |

#### Metric Category Configuration

Each metric category supports:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enabled` | bool | varies | Enable/disable category |
| `interval` | duration | varies | Polling interval |

**Recommended intervals:**
- Real-time metrics: `1m`
- Historical metrics: `5m`
- Storage metrics: `30m`
- Event tables: `30s`
- Organization metrics: `1h`

#### Custom Queries

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | yes | Metric name |
| `interval` | duration | yes | Polling interval |
| `metric_type` | string | yes | `gauge`, `counter`, or `histogram` |
| `value_column` | string | yes | Column containing metric value |
| `label_columns` | []string | yes | Columns to use as dimensions |
| `sql` | string | yes | SQL query to execute |

---

## Usage

### Running the Collector

#### Foreground (Development)

```bash
./otelcol-snowflake/otelcol-snowflake --config config.yaml
```

#### Background (Production)

Using `nohup`:
```bash
nohup ./otelcol-snowflake/otelcol-snowflake --config config.yaml > collector.log 2>&1 &
```

Using `systemd` (recommended):

Create `/etc/systemd/system/otelcol-snowflake.service`:

```ini
[Unit]
Description=OpenTelemetry Collector with Snowflake Receiver
After=network.target

[Service]
Type=simple
User=otel
Group=otel
ExecStart=/opt/otelcol-snowflake/otelcol-snowflake --config /etc/otelcol-snowflake/config.yaml
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl enable otelcol-snowflake
sudo systemctl start otelcol-snowflake
sudo systemctl status otelcol-snowflake
```

View logs:
```bash
sudo journalctl -u otelcol-snowflake -f
```

### Validating Configuration

Before deploying, validate your Snowflake environment:

```sql
-- Check Snowflake version
SELECT CURRENT_VERSION() as version;

-- Verify table access
SELECT COUNT(*) FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
WHERE START_TIME >= DATEADD(hour, -1, CURRENT_TIMESTAMP());

-- Test permissions
SHOW GRANTS TO ROLE SNOWFLAKE_MONITORING;
```

### Monitoring the Receiver

The receiver emits self-monitoring metrics. Monitor these for health:

- `snowflake.receiver.queries.total` - Should increase steadily
- `snowflake.receiver.errors.total` - Should remain low/zero
- `snowflake.receiver.success.rate` - Should stay >95%
- `snowflake.receiver.cardinality.dropped_*` - Indicates cardinality limits hit

### Upgrading

```bash
# Pull latest changes
git pull origin main

# Rebuild collector
cd snowflakereceiver
builder --config builder-config.yaml

# Restart service
sudo systemctl restart otelcol-snowflake
```

---

## Architecture

### Data Flow

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────┐
│  Snowflake  │────▶│ Snowflake        │────▶│   Backend   │
│  Database   │     │ Receiver         │     │  (Dynatrace,│
│             │     │                  │     │   Prometheus│
│             │     │ • Rate Limiter   │     │   etc.)     │
│             │     │ • Circuit Breaker│     │             │
│             │     │ • Retry Logic    │     │             │
└─────────────┘     └──────────────────┘     └─────────────┘
```

### Polling Strategy

The receiver uses a **multi-tier scraping architecture**:

1. **Real-Time Tier** (30s-2min intervals)
   - INFORMATION_SCHEMA queries
   - Event Table change data capture
   - Snowpipe API calls

2. **Near Real-Time Tier** (5-15min intervals)
   - Recent ACCOUNT_USAGE data
   - Task and warehouse metrics

3. **Historical Tier** (30min-1hr intervals)
   - Storage and cost analytics
   - Long-term trending data

This approach optimizes for:
- Low latency where it matters (operations)
- Efficient Snowflake credit usage (historical data)
- Appropriate data freshness per use case

### Cardinality Management

Circuit breaker flow:

```
New Dimension Value
        │
        ▼
   Seen Before?
    │       │
   Yes      No
    │       │
    │       ▼
    │   At Limit?
    │    │     │
    │   No    Yes (TRIP BREAKER)
    │    │     │
    │    ▼     ▼
    └──▶ Track ──▶ Aggregate
         │            │
         ▼            ▼
    Use actual   Use "high_cardinality_*"
    value        fallback value
```

### Memory Usage

Typical memory footprint:
- Baseline: ~50-100 MB
- Per 1000 tracked dimensions: ~5-10 MB
- Circuit breakers prevent unbounded growth

---

## Troubleshooting

### Common Issues

#### "invalid identifier" Errors

**Symptom:** 
```
SQL compilation error: invalid identifier 'COLUMN_NAME'
```

**Cause:** Column names vary by Snowflake version/edition.

**Solution:**
1. Check actual table structure:
   ```sql
   DESCRIBE TABLE SNOWFLAKE.ACCOUNT_USAGE.TABLE_NAME;
   ```
2. Compare against receiver expectations
3. Update queries in code or report issue

#### No Metrics Collected

**Symptom:** Receiver runs but no metrics appear in backend.

**Causes & Solutions:**

1. **Feature not enabled in Snowflake**
   - Check: `SELECT COUNT(*) FROM TABLE_NAME`
   - Solution: Normal behavior - metrics only emit when data exists

2. **Insufficient permissions**
   - Check: `SHOW GRANTS TO ROLE YOUR_ROLE`
   - Solution: Grant IMPORTED PRIVILEGES on SNOWFLAKE database

3. **Polling interval not elapsed**
   - Check: Wait for first interval to pass
   - Solution: Use shorter intervals for testing

#### Rate Limiting Warnings

**Symptom:**
```
WARN Rate limit exceeded, throttling queries
```

**Solution:** Increase `rate_limit_qps` or increase polling intervals.

#### High Cardinality Warnings

**Symptom:**
```
WARN User cardinality limit exceeded - circuit breaker activated
```

**Solution:** 
- Increase cardinality limits in config
- Accept aggregated dimensions for high-cardinality values
- Monitor `snowflake.receiver.cardinality.dropped_*` metrics

#### Connection Timeouts

**Symptom:**
```
ERROR Failed to connect to Snowflake: timeout
```

**Solutions:**
1. Check network connectivity to Snowflake
2. Verify account identifier is correct
3. Increase `query_timeout`
4. Check warehouse is running

#### High Snowflake Costs

**Symptom:** Unexpected credit consumption.

**Solutions:**
1. Use dedicated XS warehouse with 30s auto-suspend
2. Increase polling intervals for less critical metrics
3. Reduce `max_rows_per_query` to limit result set sizes
4. Monitor warehouse usage: `SELECT * FROM ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY`

### Debugging

Enable debug logging:

```yaml
service:
  telemetry:
    logs:
      level: debug
```

Check self-monitoring metrics:
```yaml
exporters:
  prometheus:
    endpoint: ":8888"
```

Then query:
```bash
curl http://localhost:8888/metrics | grep snowflake_receiver
```

### Getting Help

- **GitHub Issues:** https://github.com/npcomplete777/snowflakereceiver/issues
- **Documentation:** Check this README and inline config comments
- **Snowflake Docs:** https://docs.snowflake.com/en/sql-reference/account-usage.html

---

## Production Deployment Checklist

- [ ] Dedicated XS warehouse created with 30s auto-suspend
- [ ] Service account created with least-privilege access
- [ ] Credentials stored in secrets manager (not config file)
- [ ] Rate limiting tuned to query volume (monitor for throttling)
- [ ] Cardinality limits adjusted for environment size
- [ ] Event Tables created and configured (if needed)
- [ ] Organization metrics enabled (if multi-account Enterprise)
- [ ] Custom queries added for business-specific metrics
- [ ] Health check endpoint monitored
- [ ] Self-monitoring metrics exported
- [ ] Alerting configured for:
  - [ ] Receiver errors (`snowflake.receiver.errors.total`)
  - [ ] High cardinality dropped values
  - [ ] Query failures in Snowflake
  - [ ] Credit consumption anomalies
- [ ] Log aggregation configured
- [ ] Runbook documented for common issues
- [ ] Tested in dev/staging before production

### Recommended Warehouse Configuration

```sql
CREATE WAREHOUSE MONITORING_WH WITH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 30
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;
```

**Cost estimate:** ~6 credits/day (~180 credits/month) for comprehensive monitoring

---

## Contributing

Contributions are welcome! Please follow these guidelines:

1. **Fork the repository**
2. **Create a feature branch:** `git checkout -b feature/amazing-feature`
3. **Commit your changes:** `git commit -m 'Add amazing feature'`
4. **Push to the branch:** `git push origin feature/amazing-feature`
5. **Open a Pull Request**

### Development Setup

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/snowflakereceiver.git
cd snowflakereceiver

# Install dependencies
cd snowflakereceiver
go mod tidy

# Run tests
go test ./...

# Build
go build .
```

### Code Standards

- Follow Go best practices
- Add tests for new features
- Update documentation
- Run `go fmt` before committing
- Ensure all tests pass

---

## License

Copyright The OpenTelemetry Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


---

## Roadmap

- [ ] OAuth/SAML authentication
- [ ] Automatic column name detection
- [ ] Enhanced filtering and sampling
- [ ] Integration tests
- [ ] Performance benchmarks
- [ ] Contribution to opentelemetry-collector-contrib
