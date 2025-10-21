
package snowflakereceiver

import (
    "fmt"
    "context"
    "time"

    "go.opentelemetry.io/collector/pdata/pcommon"
    "go.opentelemetry.io/collector/pdata/pmetric"
    "go.opentelemetry.io/collector/receiver"
    "go.uber.org/zap"
)

type snowflakeScraper struct {
    logger  *zap.Logger
    config  *Config
    client  *snowflakeClient
    lastRun map[string]time.Time
}

func newSnowflakeScraper(settings receiver.Settings, config *Config) (*snowflakeScraper, error) {
    client, err := newSnowflakeClient(settings.Logger, config)
    if err != nil {
        return nil, err
    }
    
    return &snowflakeScraper{
        logger:  settings.Logger,
        config:  config,
        client:  client,
        lastRun: make(map[string]time.Time),
    }, nil
}

func (s *snowflakeScraper) Shutdown(ctx context.Context) error {
    if s.client != nil {
        return s.client.close()
    }
    return nil
}

func (s *snowflakeScraper) shouldScrape(metricName string, interval time.Duration) bool {
    lastRun, exists := s.lastRun[metricName]
    if !exists {
        return true
    }
    return time.Since(lastRun) >= interval
}

func (s *snowflakeScraper) markScraped(metricName string) {
    s.lastRun[metricName] = time.Now()
}

func (s *snowflakeScraper) scrape(ctx context.Context) (pmetric.Metrics, error) {
    if err := s.client.connect(ctx); err != nil {
        s.logger.Error("Failed to connect to Snowflake", zap.Error(err))
        return pmetric.NewMetrics(), err
    }
    
    metrics, err := s.client.queryMetrics(ctx)
    if err != nil {
        s.logger.Error("Failed to query Snowflake metrics", zap.Error(err))
        return pmetric.NewMetrics(), err
    }
    
    return s.buildMetrics(metrics), nil
}

func (s *snowflakeScraper) buildMetrics(metrics *snowflakeMetrics) pmetric.Metrics {
    md := pmetric.NewMetrics()
    resourceMetrics := md.ResourceMetrics().AppendEmpty()
    
    // Add resource attributes
    resourceAttrs := resourceMetrics.Resource().Attributes()
    resourceAttrs.PutStr("snowflake.account.name", s.config.Account)
    resourceAttrs.PutStr("snowflake.warehouse.name", s.config.Warehouse)
    resourceAttrs.PutStr("snowflake.database.name", s.config.Database)
    
    scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()
    now := pcommon.NewTimestampFromTime(time.Now())

    // ALWAYS add self-monitoring first
    s.addSelfMonitoringMetrics(scopeMetrics, now)
    
    // Add metrics based on configuration and interval scheduling
    if s.config.Metrics.CurrentQueries.Enabled && len(metrics.currentQueries) > 0 {
        interval := s.config.Metrics.CurrentQueries.GetInterval(1 * time.Minute)
        if s.shouldScrape("current_queries", interval) {
            s.addCurrentQueryMetrics(scopeMetrics, metrics.currentQueries, now)
            s.markScraped("current_queries")
        }
    }
    
    if s.config.Metrics.WarehouseLoad.Enabled && len(metrics.warehouseLoad) > 0 {
        interval := s.config.Metrics.WarehouseLoad.GetInterval(1 * time.Minute)
        if s.shouldScrape("warehouse_load", interval) {
            s.addWarehouseLoadMetrics(scopeMetrics, metrics.warehouseLoad, now)
            s.markScraped("warehouse_load")
        }
    }
    
    if s.config.Metrics.QueryHistory.Enabled && len(metrics.queryStats) > 0 {
        interval := s.config.Metrics.QueryHistory.GetInterval(5 * time.Minute)
        if s.shouldScrape("query_history", interval) {
            s.addQueryMetrics(scopeMetrics, metrics.queryStats, now)
            s.markScraped("query_history")
        }
    }
    
    if s.config.Metrics.CreditUsage.Enabled && len(metrics.creditUsage) > 0 {
        interval := s.config.Metrics.CreditUsage.GetInterval(5 * time.Minute)
        if s.shouldScrape("credit_usage", interval) {
            s.addCreditMetrics(scopeMetrics, metrics.creditUsage, now)
            s.markScraped("credit_usage")
        }
    }
    
    if s.config.Metrics.StorageMetrics.Enabled && len(metrics.storageUsage) > 0 {
        interval := s.config.Metrics.StorageMetrics.GetInterval(30 * time.Minute)
        if s.shouldScrape("storage_metrics", interval) {
            s.addStorageMetrics(scopeMetrics, metrics.storageUsage, now)
            s.markScraped("storage_metrics")
        }
    }
    
    if s.config.Metrics.LoginHistory.Enabled && len(metrics.loginHistory) > 0 {
        interval := s.config.Metrics.LoginHistory.GetInterval(10 * time.Minute)
        if s.shouldScrape("login_history", interval) {
            s.addLoginMetrics(scopeMetrics, metrics.loginHistory, now)
            s.markScraped("login_history")
        }
    }
    
    if s.config.Metrics.DataPipeline.Enabled && len(metrics.pipeUsage) > 0 {
        interval := s.config.Metrics.DataPipeline.GetInterval(10 * time.Minute)
        if s.shouldScrape("data_pipeline", interval) {
            s.addPipeMetrics(scopeMetrics, metrics.pipeUsage, now)
            s.markScraped("data_pipeline")
        }
    }
    
    if s.config.Metrics.DatabaseStorage.Enabled && len(metrics.databaseStorage) > 0 {
        interval := s.config.Metrics.DatabaseStorage.GetInterval(30 * time.Minute)
        if s.shouldScrape("database_storage", interval) {
            s.addDatabaseStorageMetrics(scopeMetrics, metrics.databaseStorage, now)
            s.markScraped("database_storage")
        }
    }
    
    if s.config.Metrics.TaskHistory.Enabled && len(metrics.taskHistory) > 0 {
        interval := s.config.Metrics.TaskHistory.GetInterval(10 * time.Minute)
        if s.shouldScrape("task_history", interval) {
            s.addTaskHistoryMetrics(scopeMetrics, metrics.taskHistory, now)
            s.markScraped("task_history")
        }
    }
    
    if s.config.Metrics.ReplicationUsage.Enabled && len(metrics.replicationUsage) > 0 {
        interval := s.config.Metrics.ReplicationUsage.GetInterval(15 * time.Minute)
        if s.shouldScrape("replication_usage", interval) {
            s.addReplicationMetrics(scopeMetrics, metrics.replicationUsage, now)
            s.markScraped("replication_usage")
        }
    }
    
    if s.config.Metrics.AutoClusteringHistory.Enabled && len(metrics.autoClusteringHistory) > 0 {
        interval := s.config.Metrics.AutoClusteringHistory.GetInterval(15 * time.Minute)
        if s.shouldScrape("auto_clustering", interval) {
            s.addAutoClusteringMetrics(scopeMetrics, metrics.autoClusteringHistory, now)
            s.markScraped("auto_clustering")
        }
    }
    
    // Event tables
    if s.config.EventTables.Enabled {
        if s.config.EventTables.QueryLogs.Enabled && len(metrics.eventQueryLogs) > 0 {
            interval := s.config.EventTables.QueryLogs.GetInterval(30 * time.Second)
            if s.shouldScrape("event_query_logs", interval) {
                s.addEventTableMetrics(scopeMetrics, metrics.eventQueryLogs, now)
                s.markScraped("event_query_logs")
            }
        }
        if s.config.EventTables.TaskLogs.Enabled && len(metrics.eventTaskLogs) > 0 {
            interval := s.config.EventTables.TaskLogs.GetInterval(30 * time.Second)
            if s.shouldScrape("event_task_logs", interval) {
                s.addEventTableMetrics(scopeMetrics, metrics.eventTaskLogs, now)
                s.markScraped("event_task_logs")
            }
        }
    }
    
    // Organization metrics
    if s.config.Organization.Enabled {
        if s.config.Organization.OrgCreditUsage.Enabled && len(metrics.orgCreditUsage) > 0 {
            interval := s.config.Organization.OrgCreditUsage.GetInterval(1 * time.Hour)
            if s.shouldScrape("org_credit_usage", interval) {
                s.addOrgCreditMetrics(scopeMetrics, metrics.orgCreditUsage, now)
                s.markScraped("org_credit_usage")
            }
        }
        if s.config.Organization.OrgStorageUsage.Enabled && len(metrics.orgStorageUsage) > 0 {
            interval := s.config.Organization.OrgStorageUsage.GetInterval(1 * time.Hour)
            if s.shouldScrape("org_storage_usage", interval) {
                s.addOrgStorageMetrics(scopeMetrics, metrics.orgStorageUsage, now)
                s.markScraped("org_storage_usage")
            }
        }
        if s.config.Organization.OrgDataTransfer.Enabled && len(metrics.orgDataTransfer) > 0 {
            interval := s.config.Organization.OrgDataTransfer.GetInterval(1 * time.Hour)
            if s.shouldScrape("org_data_transfer", interval) {
                s.addOrgDataTransferMetrics(scopeMetrics, metrics.orgDataTransfer, now)
                s.markScraped("org_data_transfer")
            }
        }
    }
    
    // Custom queries
    if s.config.CustomQueries.Enabled {
        for _, result := range metrics.customQueryResults {
            s.addCustomQueryMetrics(scopeMetrics, result, now)
        }
    }
    
    return md
}

// ============================================================================
// METRIC CREATION FUNCTIONS WITH ALL DIMENSIONS
// ============================================================================

func (s *snowflakeScraper) addCurrentQueryMetrics(scopeMetrics pmetric.ScopeMetrics, queries []currentQueryRow, now pcommon.Timestamp) {
    for _, query := range queries {
        // ⭐ Use circuit breaker for high-cardinality dimensions
        userName := s.client.cardTracker.trackUser(query.userName.String)
        schemaName := s.client.cardTracker.trackSchema(query.schemaName.String)
        databaseName := s.client.cardTracker.trackDatabase(query.databaseName.String)
        roleName := s.client.cardTracker.trackRole(query.roleName.String)
        
        // Query count metric
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.queries.current.count")
        metric.SetDescription("Current query count (last 5 minutes, REAL-TIME)")
        metric.SetUnit("{queries}")
        gauge := metric.SetEmptyGauge()
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetIntValue(query.queryCount)
        
        // Add ALL dimensions
        if query.warehouseName.Valid {
            dp.Attributes().PutStr("warehouse.name", query.warehouseName.String)
        }
        if query.warehouseSize.Valid {
            dp.Attributes().PutStr("warehouse.size", query.warehouseSize.String)
        }
        if query.queryType.Valid {
            dp.Attributes().PutStr("query.type", query.queryType.String)
        }
        if query.executionStatus.Valid {
            dp.Attributes().PutStr("execution.status", query.executionStatus.String)
        }
        dp.Attributes().PutStr("database.name", databaseName)
        dp.Attributes().PutStr("schema.name", schemaName)
        dp.Attributes().PutStr("user.name", userName)
        dp.Attributes().PutStr("role.name", roleName)
        if query.executionStatus.String == "FAILED" && query.errorCode.Valid {
            dp.Attributes().PutStr("error.code", query.errorCode.String)
        }
        dp.Attributes().PutStr("data.source", "information_schema")
        
        // Execution time metric
        if query.avgExecutionTime.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.queries.current.execution.time")
            metric.SetDescription("Average execution time for current queries")
            metric.SetUnit("ms")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetDoubleValue(query.avgExecutionTime.Float64)
            
            // Same dimensions
            if query.warehouseName.Valid {
                dp.Attributes().PutStr("warehouse.name", query.warehouseName.String)
            }
            if query.warehouseSize.Valid {
                dp.Attributes().PutStr("warehouse.size", query.warehouseSize.String)
            }
            if query.queryType.Valid {
                dp.Attributes().PutStr("query.type", query.queryType.String)
            }
            if query.executionStatus.Valid {
                dp.Attributes().PutStr("execution.status", query.executionStatus.String)
            }
            dp.Attributes().PutStr("database.name", databaseName)
            dp.Attributes().PutStr("schema.name", schemaName)
            dp.Attributes().PutStr("user.name", userName)
            dp.Attributes().PutStr("role.name", roleName)
            if query.executionStatus.String == "FAILED" && query.errorCode.Valid {
                dp.Attributes().PutStr("error.code", query.errorCode.String)
            }
            dp.Attributes().PutStr("data.source", "information_schema")
        }
        
        // Bytes scanned metric
        if query.avgBytesScanned.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.queries.current.bytes.scanned")
            metric.SetDescription("Average bytes scanned for current queries")
            metric.SetUnit("By")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetDoubleValue(query.avgBytesScanned.Float64)
            
            // Same dimensions
            if query.warehouseName.Valid {
                dp.Attributes().PutStr("warehouse.name", query.warehouseName.String)
            }
            if query.warehouseSize.Valid {
                dp.Attributes().PutStr("warehouse.size", query.warehouseSize.String)
            }
            if query.queryType.Valid {
                dp.Attributes().PutStr("query.type", query.queryType.String)
            }
            if query.executionStatus.Valid {
                dp.Attributes().PutStr("execution.status", query.executionStatus.String)
            }
            dp.Attributes().PutStr("database.name", databaseName)
            dp.Attributes().PutStr("schema.name", schemaName)
            dp.Attributes().PutStr("user.name", userName)
            dp.Attributes().PutStr("role.name", roleName)
            if query.executionStatus.String == "FAILED" && query.errorCode.Valid {
                dp.Attributes().PutStr("error.code", query.errorCode.String)
            }
            dp.Attributes().PutStr("data.source", "information_schema")
        }
    }
}


func (s *snowflakeScraper) addWarehouseLoadMetrics(scopeMetrics pmetric.ScopeMetrics, loads []warehouseLoadRow, now pcommon.Timestamp) {
    for _, load := range loads {
        // Running queries
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.warehouse.queries.running")
        metric.SetDescription("Average running queries in warehouse")
        metric.SetUnit("{queries}")
        gauge := metric.SetEmptyGauge()
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetDoubleValue(load.avgRunning.Float64)
        
        if load.warehouseName.Valid {
            dp.Attributes().PutStr("warehouse.name", load.warehouseName.String)
        }
        if load.warehouseSize.Valid {
            dp.Attributes().PutStr("warehouse.size", load.warehouseSize.String)
        }
        dp.Attributes().PutStr("data.source", "information_schema")
        
        // Queued overload
        metric = scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.warehouse.queries.queued.overload")
        metric.SetDescription("Average queries queued due to overload")
        metric.SetUnit("{queries}")
        gauge = metric.SetEmptyGauge()
        dp = gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetDoubleValue(load.avgQueuedLoad.Float64)
        
        if load.warehouseName.Valid {
            dp.Attributes().PutStr("warehouse.name", load.warehouseName.String)
        }
        if load.warehouseSize.Valid {
            dp.Attributes().PutStr("warehouse.size", load.warehouseSize.String)
        }
        dp.Attributes().PutStr("data.source", "information_schema")
        
        // Queued provisioning
        metric = scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.warehouse.queries.queued.provisioning")
        metric.SetDescription("Average queries queued for provisioning")
        metric.SetUnit("{queries}")
        gauge = metric.SetEmptyGauge()
        dp = gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetDoubleValue(load.avgQueuedProvisioning.Float64)
        
        if load.warehouseName.Valid {
            dp.Attributes().PutStr("warehouse.name", load.warehouseName.String)
        }
        if load.warehouseSize.Valid {
            dp.Attributes().PutStr("warehouse.size", load.warehouseSize.String)
        }
        dp.Attributes().PutStr("data.source", "information_schema")
        
        // Blocked queries
        metric = scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.warehouse.queries.blocked")
        metric.SetDescription("Average blocked queries")
        metric.SetUnit("{queries}")
        gauge = metric.SetEmptyGauge()
        dp = gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetDoubleValue(load.avgBlocked.Float64)
        
        if load.warehouseName.Valid {
            dp.Attributes().PutStr("warehouse.name", load.warehouseName.String)
        }
        if load.warehouseSize.Valid {
            dp.Attributes().PutStr("warehouse.size", load.warehouseSize.String)
        }
        dp.Attributes().PutStr("data.source", "information_schema")
    }
}

func (s *snowflakeScraper) addQueryMetrics(scopeMetrics pmetric.ScopeMetrics, stats []queryStatRow, now pcommon.Timestamp) {
    for _, stat := range stats {
        // Use circuit breaker for high-cardinality dimensions
        userName := s.client.cardTracker.trackUser(stat.userName.String)
        schemaName := s.client.cardTracker.trackSchema(stat.schemaName.String)
        databaseName := s.client.cardTracker.trackDatabase(stat.databaseName.String)
        roleName := s.client.cardTracker.trackRole(stat.roleName.String)
        
        // Query count
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.queries.count")
        metric.SetDescription("Total query count (ACCOUNT_USAGE)")
        metric.SetUnit("{queries}")
        gauge := metric.SetEmptyGauge()
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetIntValue(stat.queryCount)
        
        if stat.warehouseName.Valid {
            dp.Attributes().PutStr("warehouse.name", stat.warehouseName.String)
        }
        if stat.warehouseSize.Valid {
            dp.Attributes().PutStr("warehouse.size", stat.warehouseSize.String)
        }
        if stat.queryType.Valid {
            dp.Attributes().PutStr("query.type", stat.queryType.String)
        }
        if stat.executionStatus.Valid {
            dp.Attributes().PutStr("execution.status", stat.executionStatus.String)
        }
        dp.Attributes().PutStr("database.name", databaseName)
        dp.Attributes().PutStr("schema.name", schemaName)
        dp.Attributes().PutStr("user.name", userName)
        dp.Attributes().PutStr("role.name", roleName)
        if stat.executionStatus.String == "FAILED" && stat.errorCode.Valid {
            dp.Attributes().PutStr("error.code", stat.errorCode.String)
        }
        dp.Attributes().PutStr("data.source", "account_usage")
        
        // Execution time
        if stat.avgExecutionTime.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.queries.execution.time")
            metric.SetDescription("Average query execution time")
            metric.SetUnit("ms")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetDoubleValue(stat.avgExecutionTime.Float64)
            
            if stat.warehouseName.Valid {
                dp.Attributes().PutStr("warehouse.name", stat.warehouseName.String)
            }
            if stat.warehouseSize.Valid {
                dp.Attributes().PutStr("warehouse.size", stat.warehouseSize.String)
            }
            if stat.queryType.Valid {
                dp.Attributes().PutStr("query.type", stat.queryType.String)
            }
            if stat.executionStatus.Valid {
                dp.Attributes().PutStr("execution.status", stat.executionStatus.String)
            }
            dp.Attributes().PutStr("database.name", databaseName)
            dp.Attributes().PutStr("schema.name", schemaName)
            dp.Attributes().PutStr("user.name", userName)
            dp.Attributes().PutStr("role.name", roleName)
            dp.Attributes().PutStr("data.source", "account_usage")
        }
        
        // Bytes scanned
        if stat.avgBytesScanned.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.queries.bytes.scanned")
            metric.SetDescription("Average bytes scanned")
            metric.SetUnit("By")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetDoubleValue(stat.avgBytesScanned.Float64)
            
            if stat.warehouseName.Valid {
                dp.Attributes().PutStr("warehouse.name", stat.warehouseName.String)
            }
            if stat.warehouseSize.Valid {
                dp.Attributes().PutStr("warehouse.size", stat.warehouseSize.String)
            }
            if stat.queryType.Valid {
                dp.Attributes().PutStr("query.type", stat.queryType.String)
            }
            if stat.executionStatus.Valid {
                dp.Attributes().PutStr("execution.status", stat.executionStatus.String)
            }
            dp.Attributes().PutStr("database.name", databaseName)
            dp.Attributes().PutStr("schema.name", schemaName)
            dp.Attributes().PutStr("user.name", userName)
            dp.Attributes().PutStr("role.name", roleName)
            dp.Attributes().PutStr("data.source", "account_usage")
        }
        
        // Bytes written
        if stat.avgBytesWritten.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.queries.bytes.written")
            metric.SetDescription("Average bytes written")
            metric.SetUnit("By")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetDoubleValue(stat.avgBytesWritten.Float64)
            
            if stat.warehouseName.Valid {
                dp.Attributes().PutStr("warehouse.name", stat.warehouseName.String)
            }
            if stat.warehouseSize.Valid {
                dp.Attributes().PutStr("warehouse.size", stat.warehouseSize.String)
            }
            if stat.queryType.Valid {
                dp.Attributes().PutStr("query.type", stat.queryType.String)
            }
            if stat.executionStatus.Valid {
                dp.Attributes().PutStr("execution.status", stat.executionStatus.String)
            }
            dp.Attributes().PutStr("database.name", databaseName)
            dp.Attributes().PutStr("schema.name", schemaName)
            dp.Attributes().PutStr("user.name", userName)
            dp.Attributes().PutStr("role.name", roleName)
            dp.Attributes().PutStr("data.source", "account_usage")
        }
        
        // Rows produced
        if stat.avgRowsProduced.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.queries.rows.produced")
            metric.SetDescription("Average rows produced")
            metric.SetUnit("{rows}")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetDoubleValue(stat.avgRowsProduced.Float64)
            
            if stat.warehouseName.Valid {
                dp.Attributes().PutStr("warehouse.name", stat.warehouseName.String)
            }
            if stat.warehouseSize.Valid {
                dp.Attributes().PutStr("warehouse.size", stat.warehouseSize.String)
            }
            if stat.queryType.Valid {
                dp.Attributes().PutStr("query.type", stat.queryType.String)
            }
            if stat.executionStatus.Valid {
                dp.Attributes().PutStr("execution.status", stat.executionStatus.String)
            }
            dp.Attributes().PutStr("database.name", databaseName)
            dp.Attributes().PutStr("schema.name", schemaName)
            dp.Attributes().PutStr("user.name", userName)
            dp.Attributes().PutStr("role.name", roleName)
            dp.Attributes().PutStr("data.source", "account_usage")
        }
        
        // Compilation time
        if stat.avgCompilationTime.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.queries.compilation.time")
            metric.SetDescription("Average compilation time")
            metric.SetUnit("ms")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetDoubleValue(stat.avgCompilationTime.Float64)
            
            if stat.warehouseName.Valid {
                dp.Attributes().PutStr("warehouse.name", stat.warehouseName.String)
            }
            if stat.warehouseSize.Valid {
                dp.Attributes().PutStr("warehouse.size", stat.warehouseSize.String)
            }
            if stat.queryType.Valid {
                dp.Attributes().PutStr("query.type", stat.queryType.String)
            }
            if stat.executionStatus.Valid {
                dp.Attributes().PutStr("execution.status", stat.executionStatus.String)
            }
            dp.Attributes().PutStr("database.name", databaseName)
            dp.Attributes().PutStr("schema.name", schemaName)
            dp.Attributes().PutStr("user.name", userName)
            dp.Attributes().PutStr("role.name", roleName)
            dp.Attributes().PutStr("data.source", "account_usage")
        }
    }
}

func (s *snowflakeScraper) addCreditMetrics(scopeMetrics pmetric.ScopeMetrics, credits []creditUsageRow, now pcommon.Timestamp) {
    for _, credit := range credits {
        // Total credits
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.warehouse.credits.usage.total")
        metric.SetDescription("Total credits used by warehouse")
        metric.SetUnit("{credits}")
        gauge := metric.SetEmptyGauge()
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetDoubleValue(credit.totalCredits)
        
        if credit.warehouseName.Valid {
            dp.Attributes().PutStr("warehouse.name", credit.warehouseName.String)
        }
        if credit.warehouseSize.Valid {
            dp.Attributes().PutStr("warehouse.size", credit.warehouseSize.String)
        }
        dp.Attributes().PutStr("data.source", "account_usage")
        
        // Compute credits
        if credit.computeCredits.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.warehouse.credits.usage.compute")
            metric.SetDescription("Compute credits used")
            metric.SetUnit("{credits}")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetDoubleValue(credit.computeCredits.Float64)
            
            if credit.warehouseName.Valid {
                dp.Attributes().PutStr("warehouse.name", credit.warehouseName.String)
            }
            if credit.warehouseSize.Valid {
                dp.Attributes().PutStr("warehouse.size", credit.warehouseSize.String)
            }
            dp.Attributes().PutStr("data.source", "account_usage")
        }
        
        // Cloud service credits
        if credit.cloudServiceCredits.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.warehouse.credits.usage.cloud.services")
            metric.SetDescription("Cloud service credits used")
            metric.SetUnit("{credits}")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetDoubleValue(credit.cloudServiceCredits.Float64)
            
            if credit.warehouseName.Valid {
                dp.Attributes().PutStr("warehouse.name", credit.warehouseName.String)
            }
            if credit.warehouseSize.Valid {
                dp.Attributes().PutStr("warehouse.size", credit.warehouseSize.String)
            }
            dp.Attributes().PutStr("data.source", "account_usage")
        }
    }
}

func (s *snowflakeScraper) addStorageMetrics(scopeMetrics pmetric.ScopeMetrics, storage []storageUsageRow, now pcommon.Timestamp) {
    for _, store := range storage {
        // Total storage
        if store.totalStorageBytes.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.storage.bytes.total")
            metric.SetDescription("Total storage bytes")
            metric.SetUnit("By")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetDoubleValue(store.totalStorageBytes.Float64)
            dp.Attributes().PutStr("data.source", "account_usage")
        }
        
        // Stage bytes
        if store.stageBytes.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.storage.bytes.stage")
            metric.SetDescription("Stage storage bytes")
            metric.SetUnit("By")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetDoubleValue(store.stageBytes.Float64)
            dp.Attributes().PutStr("data.source", "account_usage")
        }
        
        // Failsafe bytes
        if store.failsafeBytes.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.storage.bytes.failsafe")
            metric.SetDescription("Failsafe storage bytes")
            metric.SetUnit("By")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetDoubleValue(store.failsafeBytes.Float64)
            dp.Attributes().PutStr("data.source", "account_usage")
        }
    }
}

func (s *snowflakeScraper) addLoginMetrics(scopeMetrics pmetric.ScopeMetrics, logins []loginHistoryRow, now pcommon.Timestamp) {
    for _, login := range logins {
        userName := s.client.cardTracker.trackUser(login.userName.String)
        
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.logins.count")
        metric.SetDescription("Login attempt count")
        metric.SetUnit("{logins}")
        gauge := metric.SetEmptyGauge()
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetIntValue(login.loginCount)
        
        dp.Attributes().PutStr("user.name", userName)
        dp.Attributes().PutStr("is.success", login.isSuccess)
        if login.errorCode.Valid {
            dp.Attributes().PutStr("error.code", login.errorCode.String)
        }
        if login.reportedClientType.Valid {
            dp.Attributes().PutStr("client.type", login.reportedClientType.String)
        }
        dp.Attributes().PutStr("data.source", "account_usage")
    }
}

func (s *snowflakeScraper) addPipeMetrics(scopeMetrics pmetric.ScopeMetrics, pipes []pipeUsageRow, now pcommon.Timestamp) {
    for _, pipe := range pipes {
        databaseName := s.client.cardTracker.trackDatabase(pipe.databaseName.String)
        schemaName := s.client.cardTracker.trackSchema(pipe.schemaName.String)
        
        // Credits used
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.pipe.credits.usage")
        metric.SetDescription("Pipe credits used")
        metric.SetUnit("{credits}")
        gauge := metric.SetEmptyGauge()
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetDoubleValue(pipe.totalCredits)
        
        if pipe.pipeName.Valid {
            dp.Attributes().PutStr("pipe.name", pipe.pipeName.String)
        }
        dp.Attributes().PutStr("database.name", databaseName)
        dp.Attributes().PutStr("schema.name", schemaName)
        dp.Attributes().PutStr("data.source", "account_usage")
        
        // Bytes inserted
        if pipe.bytesInserted.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.pipe.bytes.inserted")
            metric.SetDescription("Bytes inserted by pipe")
            metric.SetUnit("By")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetDoubleValue(pipe.bytesInserted.Float64)
            
            if pipe.pipeName.Valid {
                dp.Attributes().PutStr("pipe.name", pipe.pipeName.String)
            }
            dp.Attributes().PutStr("database.name", databaseName)
            dp.Attributes().PutStr("schema.name", schemaName)
            dp.Attributes().PutStr("data.source", "account_usage")
        }
        
        // Files inserted
        if pipe.filesInserted.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.pipe.files.inserted")
            metric.SetDescription("Files inserted by pipe")
            metric.SetUnit("{files}")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetIntValue(pipe.filesInserted.Int64)
            
            if pipe.pipeName.Valid {
                dp.Attributes().PutStr("pipe.name", pipe.pipeName.String)
            }
            dp.Attributes().PutStr("database.name", databaseName)
            dp.Attributes().PutStr("schema.name", schemaName)
            dp.Attributes().PutStr("data.source", "account_usage")
        }
    }
}

func (s *snowflakeScraper) addDatabaseStorageMetrics(scopeMetrics pmetric.ScopeMetrics, dbStorage []databaseStorageRow, now pcommon.Timestamp) {
    for _, db := range dbStorage {
        databaseName := s.client.cardTracker.trackDatabase(db.databaseName.String)
        
        // Database bytes
        if db.avgDatabaseBytes.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.database.storage.bytes")
            metric.SetDescription("Database storage bytes")
            metric.SetUnit("By")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetDoubleValue(db.avgDatabaseBytes.Float64)
            
            dp.Attributes().PutStr("database.name", databaseName)
            dp.Attributes().PutStr("data.source", "account_usage")
        }
        
        // Failsafe bytes
        if db.avgFailsafeBytes.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.database.failsafe.bytes")
            metric.SetDescription("Database failsafe bytes")
            metric.SetUnit("By")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetDoubleValue(db.avgFailsafeBytes.Float64)
            
            dp.Attributes().PutStr("database.name", databaseName)
            dp.Attributes().PutStr("data.source", "account_usage")
        }
    }
}

func (s *snowflakeScraper) addTaskHistoryMetrics(scopeMetrics pmetric.ScopeMetrics, tasks []taskHistoryRow, now pcommon.Timestamp) {
    for _, task := range tasks {
        databaseName := s.client.cardTracker.trackDatabase(task.databaseName.String)
        schemaName := s.client.cardTracker.trackSchema(task.schemaName.String)
        
        // Execution count
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.tasks.executions.count")
        metric.SetDescription("Task execution count")
        metric.SetUnit("{executions}")
        gauge := metric.SetEmptyGauge()
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetIntValue(task.executionCount)
        
        dp.Attributes().PutStr("database.name", databaseName)
        dp.Attributes().PutStr("schema.name", schemaName)
        if task.taskName.Valid {
            dp.Attributes().PutStr("task.name", task.taskName.String)
        }
        if task.state.Valid {
            dp.Attributes().PutStr("task.state", task.state.String)
        }
        if task.warehouseName.Valid {
            dp.Attributes().PutStr("warehouse.name", task.warehouseName.String)
        }
        if task.state.String == "FAILED" && task.errorCode.Valid {
            dp.Attributes().PutStr("error.code", task.errorCode.String)
        }
        dp.Attributes().PutStr("data.source", "account_usage")
        
        // Execution time
        if task.avgExecutionTime.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.tasks.execution.time")
            metric.SetDescription("Average task execution time")
            metric.SetUnit("s")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetDoubleValue(task.avgExecutionTime.Float64)
            
            dp.Attributes().PutStr("database.name", databaseName)
            dp.Attributes().PutStr("schema.name", schemaName)
            if task.taskName.Valid {
                dp.Attributes().PutStr("task.name", task.taskName.String)
            }
            if task.state.Valid {
                dp.Attributes().PutStr("task.state", task.state.String)
            }
            if task.warehouseName.Valid {
                dp.Attributes().PutStr("warehouse.name", task.warehouseName.String)
            }
            dp.Attributes().PutStr("data.source", "account_usage")
        }
    }
}

func (s *snowflakeScraper) addReplicationMetrics(scopeMetrics pmetric.ScopeMetrics, replications []replicationUsageRow, now pcommon.Timestamp) {
    for _, repl := range replications {
        databaseName := s.client.cardTracker.trackDatabase(repl.databaseName.String)
        
        // Credits used
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.replication.credits.usage")
        metric.SetDescription("Replication credits used")
        metric.SetUnit("{credits}")
        gauge := metric.SetEmptyGauge()
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetDoubleValue(repl.totalCredits)
        
        dp.Attributes().PutStr("database.name", databaseName)
        if repl.sourceAccount.Valid {
            dp.Attributes().PutStr("source.account", repl.sourceAccount.String)
        }
        if repl.targetAccount.Valid {
            dp.Attributes().PutStr("target.account", repl.targetAccount.String)
        }
        if repl.sourceRegion.Valid {
            dp.Attributes().PutStr("source.region", repl.sourceRegion.String)
        }
        if repl.targetRegion.Valid {
            dp.Attributes().PutStr("target.region", repl.targetRegion.String)
        }
        dp.Attributes().PutStr("data.source", "account_usage")
        
        // Bytes transferred
        if repl.bytesTransferred.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.replication.bytes.transferred")
            metric.SetDescription("Bytes transferred in replication")
            metric.SetUnit("By")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetDoubleValue(repl.bytesTransferred.Float64)
            
            dp.Attributes().PutStr("database.name", databaseName)
            if repl.sourceAccount.Valid {
                dp.Attributes().PutStr("source.account", repl.sourceAccount.String)
            }
            if repl.targetAccount.Valid {
                dp.Attributes().PutStr("target.account", repl.targetAccount.String)
            }
            if repl.sourceRegion.Valid {
                dp.Attributes().PutStr("source.region", repl.sourceRegion.String)
            }
            if repl.targetRegion.Valid {
                dp.Attributes().PutStr("target.region", repl.targetRegion.String)
            }
            dp.Attributes().PutStr("data.source", "account_usage")
        }
    }
}

func (s *snowflakeScraper) addAutoClusteringMetrics(scopeMetrics pmetric.ScopeMetrics, clusterings []autoClusteringRow, now pcommon.Timestamp) {
    for _, cluster := range clusterings {
        databaseName := s.client.cardTracker.trackDatabase(cluster.databaseName.String)
        schemaName := s.client.cardTracker.trackSchema(cluster.schemaName.String)
        
        // Credits used
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.clustering.auto.credits.usage")
        metric.SetDescription("Auto-clustering credits used")
        metric.SetUnit("{credits}")
        gauge := metric.SetEmptyGauge()
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetDoubleValue(cluster.totalCredits)
        
        dp.Attributes().PutStr("database.name", databaseName)
        dp.Attributes().PutStr("schema.name", schemaName)
        if cluster.tableName.Valid {
            dp.Attributes().PutStr("table.name", cluster.tableName.String)
        }
        if cluster.warehouseName.Valid {
            dp.Attributes().PutStr("warehouse.name", cluster.warehouseName.String)
        }
        dp.Attributes().PutStr("data.source", "account_usage")
        
        // Bytes reclustered
        if cluster.bytesReclustered.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.clustering.auto.bytes.reclustered")
            metric.SetDescription("Bytes reclustered")
            metric.SetUnit("By")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetDoubleValue(cluster.bytesReclustered.Float64)
            
            dp.Attributes().PutStr("database.name", databaseName)
            dp.Attributes().PutStr("schema.name", schemaName)
            if cluster.tableName.Valid {
                dp.Attributes().PutStr("table.name", cluster.tableName.String)
            }
            dp.Attributes().PutStr("data.source", "account_usage")
        }
        
        // Rows reclustered
        if cluster.rowsReclustered.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.clustering.auto.rows.reclustered")
            metric.SetDescription("Rows reclustered")
            metric.SetUnit("{rows}")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetDoubleValue(cluster.rowsReclustered.Float64)
            
            dp.Attributes().PutStr("database.name", databaseName)
            dp.Attributes().PutStr("schema.name", schemaName)
            if cluster.tableName.Valid {
                dp.Attributes().PutStr("table.name", cluster.tableName.String)
            }
            dp.Attributes().PutStr("data.source", "account_usage")
        }
    }
}

func (s *snowflakeScraper) addEventTableMetrics(scopeMetrics pmetric.ScopeMetrics, events []eventTableRow, now pcommon.Timestamp) {
    // Group events by type and severity
    eventCounts := make(map[string]map[string]int64)
    
    for _, event := range events {
        if _, exists := eventCounts[event.eventType]; !exists {
            eventCounts[event.eventType] = make(map[string]int64)
        }
        severity := "UNKNOWN"
        if event.severity.Valid {
            severity = event.severity.String
        }
        eventCounts[event.eventType][severity]++
    }
    
    for eventType, severityCounts := range eventCounts {
        for severity, count := range severityCounts {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.events." + eventType + ".count")
            metric.SetDescription("Event count by type and severity")
            metric.SetUnit("{events}")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetIntValue(count)
            
            dp.Attributes().PutStr("event.type", eventType)
            dp.Attributes().PutStr("severity", severity)
            dp.Attributes().PutStr("data.source", "event_table")
        }
    }
}

func (s *snowflakeScraper) addOrgCreditMetrics(scopeMetrics pmetric.ScopeMetrics, orgCredits []orgCreditUsageRow, now pcommon.Timestamp) {
    for _, org := range orgCredits {
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.organization.credits.usage")
        metric.SetDescription("Organization credits usage")
        metric.SetUnit("{credits}")
        gauge := metric.SetEmptyGauge()
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetDoubleValue(org.totalCredits)
        
        if org.organizationName.Valid {
            dp.Attributes().PutStr("organization.name", org.organizationName.String)
        }
        if org.accountName.Valid {
            dp.Attributes().PutStr("account.name", org.accountName.String)
        }
        if org.serviceType.Valid {
            dp.Attributes().PutStr("service.type", org.serviceType.String)
        }
        dp.Attributes().PutStr("data.source", "organization_usage")
    }
}

func (s *snowflakeScraper) addOrgStorageMetrics(scopeMetrics pmetric.ScopeMetrics, orgStorage []orgStorageUsageRow, now pcommon.Timestamp) {
    for _, org := range orgStorage {
        if org.avgStorageBytes.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.organization.storage.bytes.total")
            metric.SetDescription("Organization total storage bytes")
            metric.SetUnit("By")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetDoubleValue(org.avgStorageBytes.Float64)
            
            if org.organizationName.Valid {
                dp.Attributes().PutStr("organization.name", org.organizationName.String)
            }
            if org.accountName.Valid {
                dp.Attributes().PutStr("account.name", org.accountName.String)
            }
            dp.Attributes().PutStr("data.source", "organization_usage")
        }
        
        if org.avgStageBytes.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.organization.storage.bytes.stage")
            metric.SetDescription("Organization stage storage bytes")
            metric.SetUnit("By")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetDoubleValue(org.avgStageBytes.Float64)
            
            if org.organizationName.Valid {
                dp.Attributes().PutStr("organization.name", org.organizationName.String)
            }
            if org.accountName.Valid {
                dp.Attributes().PutStr("account.name", org.accountName.String)
            }
            dp.Attributes().PutStr("data.source", "organization_usage")
        }
        
        if org.avgFailsafeBytes.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.organization.storage.bytes.failsafe")
            metric.SetDescription("Organization failsafe storage bytes")
            metric.SetUnit("By")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetDoubleValue(org.avgFailsafeBytes.Float64)
            
            if org.organizationName.Valid {
                dp.Attributes().PutStr("organization.name", org.organizationName.String)
            }
            if org.accountName.Valid {
                dp.Attributes().PutStr("account.name", org.accountName.String)
            }
            dp.Attributes().PutStr("data.source", "organization_usage")
        }
    }
}

func (s *snowflakeScraper) addOrgDataTransferMetrics(scopeMetrics pmetric.ScopeMetrics, transfers []orgDataTransferRow, now pcommon.Timestamp) {
    for _, transfer := range transfers {
        if transfer.totalBytesTransferred.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.organization.data.transfer.bytes")
            metric.SetDescription("Organization data transfer bytes")
            metric.SetUnit("By")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetDoubleValue(transfer.totalBytesTransferred.Float64)
            
            if transfer.organizationName.Valid {
                dp.Attributes().PutStr("organization.name", transfer.organizationName.String)
            }
            if transfer.sourceAccountName.Valid {
                dp.Attributes().PutStr("source.account", transfer.sourceAccountName.String)
            }
            if transfer.targetAccountName.Valid {
                dp.Attributes().PutStr("target.account", transfer.targetAccountName.String)
            }
            if transfer.sourceRegion.Valid {
                dp.Attributes().PutStr("source.region", transfer.sourceRegion.String)
            }
            if transfer.targetRegion.Valid {
                dp.Attributes().PutStr("target.region", transfer.targetRegion.String)
            }
            dp.Attributes().PutStr("data.source", "organization_usage")
        }
    }
}



func (s *snowflakeScraper) addOrgContractMetrics(scopeMetrics pmetric.ScopeMetrics, contracts []orgContractUsageRow, now pcommon.Timestamp) {
    for _, contract := range contracts {
        // Credits used metric (plain float64)
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.organization.contract.credits.used")
        metric.SetDescription("Organization contract credits used")
        metric.SetUnit("{credits}")
        gauge := metric.SetEmptyGauge()
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetDoubleValue(contract.totalCreditsUsed)
        
        if contract.organizationName.Valid {
            dp.Attributes().PutStr("organization.name", contract.organizationName.String)
        }
        if contract.contractNumber.Valid {
            dp.Attributes().PutInt("contract.number", contract.contractNumber.Int64)
        }
        dp.Attributes().PutStr("data.source", "organization_usage")
        
        // Credits billed metric (sql.NullFloat64)
        if contract.totalCreditsBilled.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.organization.contract.credits.billed")
            metric.SetDescription("Organization contract credits billed")
            metric.SetUnit("{credits}")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetDoubleValue(contract.totalCreditsBilled.Float64)
            
            if contract.organizationName.Valid {
                dp.Attributes().PutStr("organization.name", contract.organizationName.String)
            }
            if contract.contractNumber.Valid {
                dp.Attributes().PutInt("contract.number", contract.contractNumber.Int64)
            }
            dp.Attributes().PutStr("data.source", "organization_usage")
        }
    }
}
func (s *snowflakeScraper) addCustomQueryMetrics(scopeMetrics pmetric.ScopeMetrics, result customQueryResult, now pcommon.Timestamp) {
    for _, row := range result.rows {
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.custom." + result.name)
        metric.SetDescription("Custom query metric: " + result.name)
        metric.SetUnit("1")
        
        switch result.metricType {
        case "gauge":
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            
            // Try to extract value
            for k, v := range row {
                switch val := v.(type) {
                case float64:
                    dp.SetDoubleValue(val)
                case int64:
                    dp.SetIntValue(val)
                case int:
                    dp.SetIntValue(int64(val))
                }
                // Add all columns as attributes
                dp.Attributes().PutStr(k, fmt.Sprintf("%v", v))
            }
        }
    }
}

// ============================================================================
// SELF-MONITORING METRICS
// ============================================================================

