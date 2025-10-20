# Snowflake OpenTelemetry Receiver

A production-ready OpenTelemetry Collector receiver for comprehensive Snowflake monitoring. Collects 100+ metrics from Snowflake's ACCOUNT_USAGE schema including query performance, credit consumption, storage usage, login activity, and data pipeline metrics.

## Table of Contents
- [Features](#features)
- [Quick Start](#quick-start)
- [Understanding ACCOUNT_USAGE Latency](#understanding-account_usage-latency)
- [Collection Interval Strategy](#collection-interval-strategy)
- [Configuration Examples](#configuration-examples)
- [Metrics Reference](#metrics-reference)
- [Troubleshooting](#troubleshooting)

## Features

✅ **110+ Metrics Collected**
- Query performance (execution time, bytes scanned/written, compilation time)
- Credit usage tracking (total, compute, cloud services per warehouse)
- Storage metrics (total, stage, failsafe, per-database breakdown)
- Login monitoring (success/failure with error codes)
- Data pipeline metrics (Snowpipe usage, bytes/files inserted)

✅ **Configurable & Efficient**
- Toggle individual metric categories
- Configurable collection intervals
- Minimal warehouse credit consumption (metadata queries only)

✅ **Production Ready**
- Error handling and automatic retries
- Detailed logging for troubleshooting
- Resource attributes for filtering and grouping

⚠️ **Current Limitation**: Single collection interval applies to all enabled metrics. Per-metric intervals not yet supported.

## Quick Start

### Prerequisites
- Snowflake account with ACCOUNTADMIN role (or ACCOUNT_USAGE access)
- OpenTelemetry Collector Builder (ocb) v0.135.0+
- Go 1.24+

### Build & Run
```bash
# Clone and build
git clone https://github.com/npcomplete777/snowflakereceiver.git
cd snowflakereceiver
ocb --config builder-config-snowflake.yaml

# Create config (see examples below)
vi config.yaml

# Run collector
./otelcol-snowflake/otelcol-snowflake --config config.yaml
```

## Understanding ACCOUNT_USAGE Latency

**CRITICAL CONCEPT**: All Snowflake ACCOUNT_USAGE views have **45 minutes to 3+ hours built-in latency**.

This is a Snowflake platform limitation, not a receiver limitation. Snowflake aggregates data in 24-hour windows before making it available in ACCOUNT_USAGE.

### What This Means
- Queries run at 10:00 AM may not appear until 10:45 AM or later
- Storage metrics update once per day
- Real-time operational monitoring is not possible with ACCOUNT_USAGE
- Polling every 30 seconds vs 5 minutes yields the same data

### Update Frequencies by Table

| Table | Update Frequency | Recommended Poll Interval |
|-------|-----------------|---------------------------|
| `QUERY_HISTORY` | Continuous with 45min-3hr lag | 5-10 minutes |
| `WAREHOUSE_METERING_HISTORY` | Continuous with 45min-3hr lag | 5-10 minutes |
| `LOGIN_HISTORY` | Continuous with 45min-3hr lag | 5-15 minutes |
| `PIPE_USAGE_HISTORY` | Continuous with 45min-3hr lag | 10-30 minutes |
| `STORAGE_USAGE` | Daily aggregation | 30-60 minutes |
| `DATABASE_STORAGE_USAGE_HISTORY` | Daily aggregation | 30-60 minutes |

## Collection Interval Strategy

### Current Implementation
**Single interval applies to all enabled metrics.** Choose based on your most critical metrics:
```yaml
collection_intervals:
  historical: "5m"  # Applied to ALL enabled metric categories
```

### Recommended Intervals by Use Case

#### 🔥 Aggressive Monitoring (High Volume)
**Use Case**: Detailed query analysis, frequent alerting, high query volume environments
```yaml
collection_intervals:
  historical: "2m"
```
- **Pros**: More granular data, faster detection of issues
- **Cons**: Higher data volume, more API calls
- **Best For**: Production troubleshooting, performance optimization

#### ⚖️ Balanced (Recommended Default)
**Use Case**: General monitoring, cost tracking, most production workloads
```yaml
collection_intervals:
  historical: "5m"
```
- **Pros**: Good balance of freshness and efficiency
- **Cons**: None significant
- **Best For**: Standard production monitoring

#### 💰 Cost-Conscious (Low Volume)
**Use Case**: Long-term trends, cost reporting, low-activity accounts
```yaml
collection_intervals:
  historical: "15m"
```
- **Pros**: Lower data volume, reduced costs
- **Cons**: Less granular historical view
- **Best For**: Development/staging environments, cost analysis

#### 📊 Storage & Billing Only (Minimal)
**Use Case**: Just track credits and storage, no query details
```yaml
collection_intervals:
  historical: "30m"

metrics:
  query_history: false      # Disable detailed query metrics
  credit_usage: true        # Keep credit tracking
  storage_metrics: true     # Keep storage tracking
  login_history: false
  data_pipeline: false
  database_storage: true
```
- **Pros**: Minimal data volume
- **Cons**: No operational insights
- **Best For**: Cost-only monitoring

### Future Enhancement
Per-metric intervals planned for future release:
```yaml
# NOT YET SUPPORTED - Coming Soon
collection_intervals:
  query_history: "2m"        # Fast-changing operational metrics
  credit_usage: "5m"         # Credit accumulation
  storage_metrics: "30m"     # Daily storage updates
  login_history: "10m"       # Security monitoring
  data_pipeline: "10m"       # Pipeline activity
  database_storage: "1h"     # Daily database storage
```

## Configuration Examples

### Example 1: Production Monitoring (Dynatrace)
**Scenario**: Monitor production Snowflake with detailed query and credit metrics
```yaml
receivers:
  snowflake:
    # Connection (CRITICAL: Use orgname-accountname format, lowercase)
    user: "monitoring_user"
    password: "${SNOWFLAKE_PASSWORD}"  # Use env var for security
    account: "myorg-prod123"           # Format: orgname-accountname
    warehouse: "COMPUTE_WH"
    database: "SNOWFLAKE"
    schema: "ACCOUNT_USAGE"
    role: "ACCOUNTADMIN"
    
    # Collection: 5min balanced interval
    collection_intervals:
      historical: "5m"
    
    # Enable all operational metrics
    metrics:
      query_history: true       # Query performance details
      credit_usage: true        # Warehouse credit consumption
      storage_metrics: true     # Account storage usage
      login_history: true       # Authentication monitoring
      data_pipeline: true       # Snowpipe activity
      database_storage: true    # Per-database storage

processors:
  batch:
    timeout: 10s
    send_batch_size: 1000

exporters:
  otlphttp:
    endpoint: "https://abc12345.live.dynatrace.com/api/v2/otlp"
    headers:
      Authorization: "Api-Token ${DYNATRACE_TOKEN}"
    timeout: 30s
    retry_on_failure:
      enabled: true
      max_elapsed_time: 300s

service:
  pipelines:
    metrics:
      receivers: [snowflake]
      processors: [batch]
      exporters: [otlphttp]
```

### Example 2: Cost Optimization Focus
**Scenario**: Track credits and storage only, minimal data volume
```yaml
receivers:
  snowflake:
    user: "billing_monitor"
    password: "${SNOWFLAKE_PASSWORD}"
    account: "myorg-prod123"
    warehouse: "COMPUTE_WH"
    database: "SNOWFLAKE"
    schema: "ACCOUNT_USAGE"
    
    # Longer interval for cost data
    collection_intervals:
      historical: "15m"
    
    # Only credit and storage metrics
    metrics:
      query_history: false      # Disable to reduce volume
      credit_usage: true        # ✓ Track warehouse costs
      storage_metrics: true     # ✓ Track storage costs
      login_history: false
      data_pipeline: false
      database_storage: true    # ✓ Per-database breakdown

processors:
  batch:
    timeout: 10s

exporters:
  otlphttp:
    endpoint: "https://your-backend/v1/metrics"
    headers:
      Authorization: "Bearer ${BACKEND_TOKEN}"

service:
  pipelines:
    metrics:
      receivers: [snowflake]
      processors: [batch]
      exporters: [otlphttp]
```

### Example 3: Development/Testing
**Scenario**: Test receiver with debug output, frequent polling
```yaml
receivers:
  snowflake:
    user: "dev_user"
    password: "DevPassword123!"
    account: "myorg-dev456"
    warehouse: "DEV_WH"
    database: "SNOWFLAKE"
    schema: "ACCOUNT_USAGE"
    
    # Fast polling for testing
    collection_intervals:
      historical: "30s"
    
    # All metrics enabled for validation
    metrics:
      query_history: true
      credit_usage: true
      storage_metrics: true
      login_history: true
      data_pipeline: true
      database_storage: true

processors:
  batch:
    timeout: 10s

exporters:
  debug:
    verbosity: detailed  # Print all metrics to console

service:
  telemetry:
    logs:
      level: debug  # Verbose logging
  pipelines:
    metrics:
      receivers: [snowflake]
      processors: [batch]
      exporters: [debug]
```

### Example 4: Multi-Warehouse Monitoring
**Scenario**: Monitor specific warehouse performance
```yaml
receivers:
  # Separate receiver per warehouse for isolation
  snowflake/warehouse_analytics:
    user: "monitoring_user"
    password: "${SNOWFLAKE_PASSWORD}"
    account: "myorg-prod123"
    warehouse: "ANALYTICS_WH"  # Specific warehouse
    database: "SNOWFLAKE"
    schema: "ACCOUNT_USAGE"
    collection_intervals:
      historical: "5m"
    metrics:
      query_history: true
      credit_usage: true
      storage_metrics: false
      login_history: false
      data_pipeline: false
      database_storage: false
  
  snowflake/warehouse_etl:
    user: "monitoring_user"
    password: "${SNOWFLAKE_PASSWORD}"
    account: "myorg-prod123"
    warehouse: "ETL_WH"  # Different warehouse
    database: "SNOWFLAKE"
    schema: "ACCOUNT_USAGE"
    collection_intervals:
      historical: "5m"
    metrics:
      query_history: true
      credit_usage: true
      storage_metrics: false
      login_history: false
      data_pipeline: true  # ETL warehouse uses Snowpipe
      database_storage: false

processors:
  batch:
    timeout: 10s

exporters:
  otlphttp:
    endpoint: "https://your-backend/v1/metrics"

service:
  pipelines:
    metrics:
      receivers: [snowflake/warehouse_analytics, snowflake/warehouse_etl]
      processors: [batch]
      exporters: [otlphttp]
```

## Metrics Reference

### Query Performance Metrics (query_history: true)

| Metric Name | Type | Unit | Description | Attributes |
|-------------|------|------|-------------|------------|
| `snowflake.query.count` | Gauge | {queries} | Number of queries executed | warehouse_name, query_type, execution_status |
| `snowflake.query.execution_time` | Gauge | ms | Average query execution time | warehouse_name, query_type |
| `snowflake.query.bytes_scanned` | Gauge | By | Average bytes scanned per query | warehouse_name, query_type |
| `snowflake.query.bytes_written` | Gauge | By | Average bytes written per query | warehouse_name, query_type |
| `snowflake.query.bytes_deleted` | Gauge | By | Average bytes deleted per query | warehouse_name, query_type |
| `snowflake.query.rows_produced` | Gauge | {rows} | Average rows produced per query | warehouse_name, query_type |
| `snowflake.query.compilation_time` | Gauge | ms | Average query compilation time | warehouse_name, query_type |

**Query Types**: SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, SHOW, USE, GRANT, CALL, etc.
**Execution Status**: SUCCESS, FAIL

### Credit Usage Metrics (credit_usage: true)

| Metric Name | Type | Unit | Description | Attributes |
|-------------|------|------|-------------|------------|
| `snowflake.warehouse.credit_usage` | Gauge | {credits} | Total warehouse credit usage | warehouse_name |
| `snowflake.warehouse.credit_usage.compute` | Gauge | {credits} | Compute credit usage | warehouse_name |
| `snowflake.warehouse.credit_usage.cloud_services` | Gauge | {credits} | Cloud services credit usage | warehouse_name |

### Storage Metrics (storage_metrics: true)

| Metric Name | Type | Unit | Description |
|-------------|------|------|-------------|
| `snowflake.storage.total` | Gauge | By | Total account storage bytes |
| `snowflake.storage.stage` | Gauge | By | Stage storage bytes |
| `snowflake.storage.failsafe` | Gauge | By | Failsafe storage bytes |

### Database Storage Metrics (database_storage: true)

| Metric Name | Type | Unit | Description | Attributes |
|-------------|------|------|-------------|------------|
| `snowflake.database.storage` | Gauge | By | Database storage bytes | database_name |
| `snowflake.database.failsafe` | Gauge | By | Database failsafe bytes | database_name |

### Login Metrics (login_history: true)

| Metric Name | Type | Unit | Description | Attributes |
|-------------|------|------|-------------|------------|
| `snowflake.login.count` | Gauge | {logins} | Number of login attempts | is_success, error_code |

### Data Pipeline Metrics (data_pipeline: true)

| Metric Name | Type | Unit | Description | Attributes |
|-------------|------|------|-------------|------------|
| `snowflake.pipe.credit_usage` | Gauge | {credits} | Snowpipe credit usage | pipe_name |
| `snowflake.pipe.bytes_inserted` | Gauge | By | Bytes inserted via Snowpipe | pipe_name |
| `snowflake.pipe.files_inserted` | Gauge | {files} | Files inserted via Snowpipe | pipe_name |

### Resource Attributes
All metrics include:
- `snowflake.account.name`: Snowflake account identifier
- `snowflake.warehouse.name`: Warehouse name (from config)
- `snowflake.database.name`: Database name (usually SNOWFLAKE)

## Account Name Format

**CRITICAL**: Account must be in `orgname-accountname` format (lowercase).

### Finding Your Account Identifier

1. Log into Snowflake web UI
2. Look at browser URL: `https://app.snowflake.com/{orgname}/{accountname}/`
3. Convert to format: `{orgname}-{accountname}` (all lowercase)

**Examples**:
- URL: `https://app.snowflake.com/vhrlbph/io32862/` → Account: `vhrlbph-io32862`
- URL: `https://app.snowflake.com/acme/prod123/` → Account: `acme-prod123`
- URL: `https://app.snowflake.com/MyOrg/DevAccount/` → Account: `myorg-devaccount`

### Common Format Errors
❌ `IO32862` (missing org, uppercase)
❌ `VHRLBPH.IO32862` (wrong separator)
❌ `vhrlbph_io32862` (underscore instead of dash)
✅ `vhrlbph-io32862` (correct format)

## Running the Collector

### Foreground (Development)
```bash
./otelcol-snowflake/otelcol-snowflake --config config.yaml
```

### Background (Production)
```bash
# Start in background
nohup ./otelcol-snowflake/otelcol-snowflake --config config.yaml > collector.log 2>&1 &

# Check process
ps aux | grep otelcol-snowflake

# Monitor logs
tail -f collector.log

# Stop collector
kill $(ps aux | grep 'otelcol-snowflake' | grep -v grep | awk '{print $2}')
```

### Systemd Service (Linux)
```bash
# Create service file
sudo vi /etc/systemd/system/snowflake-otel.service
```
```ini
[Unit]
Description=Snowflake OpenTelemetry Collector
After=network.target

[Service]
Type=simple
User=otel
ExecStart=/opt/otel/otelcol-snowflake --config=/opt/otel/config.yaml
Restart=on-failure
RestartSec=30s

[Install]
WantedBy=multi-user.target
```
```bash
# Enable and start
sudo systemctl daemon-reload
sudo systemctl enable snowflake-otel
sudo systemctl start snowflake-otel
sudo systemctl status snowflake-otel
```

## Troubleshooting

### Authentication Error (261004)
```
Error: 261004 (08004): failed to auth for unknown reason. HTTP: 404
```

**Cause**: Incorrect account format
**Solution**: 
1. Verify account format: `orgname-accountname` (lowercase, dash separator)
2. Check URL in Snowflake web UI: `https://app.snowflake.com/{org}/{account}/`
3. Test connection with curl (see README curl example)

### No Metrics Collected
**Symptoms**: Collector runs but no metrics appear

**Causes & Solutions**:
1. **New account with no data**: ACCOUNT_USAGE needs historical activity
   - Solution: Run queries in Snowflake UI, wait 45+ minutes
2. **All metrics disabled**: Check `metrics:` config
   - Solution: Enable at least one category
3. **Warehouse not running**: Some queries require active warehouse
   - Solution: Ensure warehouse is running and auto-suspend is reasonable

### Missing Storage Metrics
**Symptoms**: Query and credit metrics work, but no storage metrics

**Causes**:
- Storage metrics (`STORAGE_USAGE` table) update once per day
- New accounts may not have data yet
- Table requires 24+ hours of account history

**Solutions**:
```sql
-- Check if storage data exists
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE 
ORDER BY USAGE_DATE DESC LIMIT 10;
```

If no rows returned, wait 24-48 hours after account creation.

### High Cardinality / Data Volume
**Symptoms**: Too many unique metric combinations, high storage costs

**Solutions**:
1. Increase collection interval (5m → 15m)
2. Disable unused categories:
```yaml
   metrics:
     query_history: true   # Keep
     credit_usage: true    # Keep
     storage_metrics: false  # Disable
     login_history: false    # Disable
     data_pipeline: false    # Disable
     database_storage: false # Disable
```
3. Filter in backend (if supported)

### Debug Logging
Enable detailed logging to diagnose issues:
```yaml
service:
  telemetry:
    logs:
      level: debug  # Verbose logging
  
exporters:
  debug:
    verbosity: detailed  # Print metrics to console
```

Then check logs for specific error messages.

## Performance & Cost Considerations

### Credit Consumption
- **Receiver queries**: Metadata only, minimal credit usage (~0.001 credits per scrape)
- **Cost impact**: Negligible for most workloads
- **Recommendation**: Monitor with `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` to verify

### Data Volume Estimates
Based on typical workload with 5m interval:

| Configuration | Metrics/Scrape | Daily Metrics | Monthly Metrics |
|---------------|----------------|---------------|-----------------|
| All enabled | ~110 | ~31,000 | ~950,000 |
| Query + Credits | ~80 | ~23,000 | ~690,000 |
| Credits only | ~10 | ~2,900 | ~87,000 |

### Optimization Tips
1. **Disable unused categories**: Reduces by 30-70%
2. **Increase interval**: 5m→15m reduces by 66%
3. **Filter at export**: Use processor to drop unnecessary attributes
4. **Aggregate in backend**: Use backend aggregation rules

## Development

### Project Structure
```
snowflakereceiver/
├── receiver/
│   ├── client.go          # Snowflake API client & queries
│   ├── config.go          # Configuration structs & validation
│   ├── factory.go         # OTel receiver factory
│   ├── scraper.go         # Metric scraping & transformation
│   ├── metadata.yaml      # Metric definitions (unused currently)
│   └── go.mod             # Go dependencies
├── builder-config-snowflake.yaml  # OCB build configuration
├── config-example.yaml    # Example collector config
└── README.md
```

### Building from Source
```bash
# Install dependencies
go mod tidy

# Build custom collector
ocb --config builder-config-snowflake.yaml

# Test
./otelcol-snowflake/otelcol-snowflake --version
```

### Adding New Metrics

1. **Add query to client.go**:
```go
func (c *snowflakeClient) queryNewMetric(ctx context.Context) ([]newMetricRow, error) {
    query := `SELECT ... FROM SNOWFLAKE.ACCOUNT_USAGE.NEW_TABLE`
    // Implementation
}
```

2. **Update metric building in scraper.go**:
```go
func (s *snowflakeScraper) addNewMetric(scopeMetrics pmetric.ScopeMetrics, data []newMetricRow, now pcommon.Timestamp) {
    // Metric transformation
}
```

3. **Add config toggle in config.go**:
```go
type MetricsConfig struct {
    NewMetric bool `mapstructure:"new_metric"`
}
```

4. **Rebuild**:
```bash
ocb --config builder-config-snowflake.yaml
```

## Roadmap

### Phase 1: ACCOUNT_USAGE Expansion ✅
- [x] Query performance metrics
- [x] Credit usage tracking
- [x] Storage metrics
- [x] Login history
- [x] Data pipeline metrics
- [x] Database storage breakdown

### Phase 2: Advanced Features (Planned)
- [ ] Per-metric collection intervals
- [ ] Event Tables support (near real-time)
- [ ] INFORMATION_SCHEMA queries (current state)
- [ ] Key-pair authentication
- [ ] Organization-level metrics
- [ ] Replication & clustering metrics

### Phase 3: Enterprise Features (Future)
- [ ] Multi-account support
- [ ] Custom SQL queries
- [ ] Alert rules & thresholds
- [ ] Metric aggregation & rollups

## Contributing

Contributions welcome! Areas for contribution:
- Additional ACCOUNT_USAGE metrics
- Per-metric interval support
- Event Tables integration
- Documentation improvements
- Bug reports & fixes

## License

Apache 2.0

## References

- [Snowflake ACCOUNT_USAGE Documentation](https://docs.snowflake.com/en/sql-reference/account-usage)
- [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/)
- [OpenTelemetry Collector Builder](https://github.com/open-telemetry/opentelemetry-collector/tree/main/cmd/builder)
- [Snowflake Query Performance](https://docs.snowflake.com/en/user-guide/ui-snowsight-activity)
