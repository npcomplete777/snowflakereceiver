package snowflakereceiver

import (
    "context"
    "fmt"
    "strconv"
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

// Shutdown closes the database connection
func (s *snowflakeScraper) Shutdown(ctx context.Context) error {
    s.logger.Info("Shutting down Snowflake scraper")
    if s.client != nil {
        if err := s.client.Close(); err != nil {
            s.logger.Error("Failed to close Snowflake connection", zap.Error(err))
            return err
        }
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
    
    resourceAttrs := resourceMetrics.Resource().Attributes()
    resourceAttrs.PutStr("snowflake.account.name", s.config.Account)
    resourceAttrs.PutStr("snowflake.warehouse.name", s.config.Warehouse)
    resourceAttrs.PutStr("snowflake.database.name", s.config.Database)
    
    scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()
    now := pcommon.NewTimestampFromTime(time.Now())

    // Add self-monitoring metrics FIRST
    s.addSelfMonitoringMetrics(scopeMetrics, now)
    
    // Standard metrics with per-metric intervals
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
        if s.shouldScrape("auto_clustering_history", interval) {
            s.addAutoClusteringMetrics(scopeMetrics, metrics.autoClusteringHistory, now)
            s.markScraped("auto_clustering_history")
        }
    }
    
    // EVENT TABLES - REAL-TIME (seconds latency!)
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
        
        if s.config.EventTables.FunctionLogs.Enabled && len(metrics.eventFunctionLogs) > 0 {
            interval := s.config.EventTables.FunctionLogs.GetInterval(30 * time.Second)
            if s.shouldScrape("event_function_logs", interval) {
                s.addEventTableMetrics(scopeMetrics, metrics.eventFunctionLogs, now)
                s.markScraped("event_function_logs")
            }
        }
        
        if s.config.EventTables.ProcedureLogs.Enabled && len(metrics.eventProcedureLogs) > 0 {
            interval := s.config.EventTables.ProcedureLogs.GetInterval(30 * time.Second)
            if s.shouldScrape("event_procedure_logs", interval) {
                s.addEventTableMetrics(scopeMetrics, metrics.eventProcedureLogs, now)
                s.markScraped("event_procedure_logs")
            }
        }
    }
    
    // ORGANIZATION METRICS
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
        
        if s.config.Organization.OrgContractUsage.Enabled && len(metrics.orgContractUsage) > 0 {
            interval := s.config.Organization.OrgContractUsage.GetInterval(12 * time.Hour)
            if s.shouldScrape("org_contract_usage", interval) {
                s.addOrgContractMetrics(scopeMetrics, metrics.orgContractUsage, now)
                s.markScraped("org_contract_usage")
            }
        }
    }
    
    // CUSTOM QUERIES
    if s.config.CustomQueries.Enabled && len(metrics.customQueryResults) > 0 {
        for _, result := range metrics.customQueryResults {
            s.addCustomQueryMetrics(scopeMetrics, result, now)
        }
    }
    
    return md
}

// ========== INFORMATION_SCHEMA Metrics (REAL-TIME) ==========

func (s *snowflakeScraper) addCurrentQueryMetrics(scopeMetrics pmetric.ScopeMetrics, queries []currentQueryRow, now pcommon.Timestamp) {
    for _, query := range queries {
        // Query count metric
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.queries.current.count")
        metric.SetDescription("Current query count (last 5 minutes, REAL-TIME)")
        metric.SetUnit("{queries}")
        gauge := metric.SetEmptyGauge()
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetIntValue(query.queryCount)
        if query.warehouseName.Valid {
            dp.Attributes().PutStr("warehouse.name", query.warehouseName.String)
        }
        if query.queryType.Valid {
            dp.Attributes().PutStr("query.type", query.queryType.String)
        }
        if query.executionStatus.Valid {
            dp.Attributes().PutStr("execution.status", query.executionStatus.String)
        }
        dp.Attributes().PutStr("data.source", "information_schema")
        
        // Execution time metric
        if query.avgExecutionTime.Valid {
            execMetric := scopeMetrics.Metrics().AppendEmpty()
            execMetric.SetName("snowflake.queries.current.execution.time")
            execMetric.SetDescription("Current query execution time (REAL-TIME)")
            execMetric.SetUnit("ms")
            execGauge := execMetric.SetEmptyGauge()
            execDp := execGauge.DataPoints().AppendEmpty()
            execDp.SetTimestamp(now)
            execDp.SetIntValue(int64(query.avgExecutionTime.Float64))
            if query.warehouseName.Valid {
                execDp.Attributes().PutStr("warehouse.name", query.warehouseName.String)
            }
            if query.queryType.Valid {
                execDp.Attributes().PutStr("query.type", query.queryType.String)
            }
            execDp.Attributes().PutStr("data.source", "information_schema")
        }
        
        // Bytes scanned metric
        if query.avgBytesScanned.Valid {
            bytesMetric := scopeMetrics.Metrics().AppendEmpty()
            bytesMetric.SetName("snowflake.queries.current.bytes.scanned")
            bytesMetric.SetDescription("Current query bytes scanned (REAL-TIME)")
            bytesMetric.SetUnit("By")
            bytesGauge := bytesMetric.SetEmptyGauge()
            bytesDp := bytesGauge.DataPoints().AppendEmpty()
            bytesDp.SetTimestamp(now)
            bytesDp.SetIntValue(int64(query.avgBytesScanned.Float64))
            if query.warehouseName.Valid {
                bytesDp.Attributes().PutStr("warehouse.name", query.warehouseName.String)
            }
            if query.queryType.Valid {
                bytesDp.Attributes().PutStr("query.type", query.queryType.String)
            }
            bytesDp.Attributes().PutStr("data.source", "information_schema")
        }
    }
}

func (s *snowflakeScraper) addWarehouseLoadMetrics(scopeMetrics pmetric.ScopeMetrics, loads []warehouseLoadRow, now pcommon.Timestamp) {
    for _, load := range loads {
        // Running queries
        if load.avgRunning.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.warehouse.queries.running")
            metric.SetDescription("Average queries running (REAL-TIME)")
            metric.SetUnit("{queries}")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetDoubleValue(load.avgRunning.Float64)
            if load.warehouseName.Valid {
                dp.Attributes().PutStr("warehouse.name", load.warehouseName.String)
            }
            dp.Attributes().PutStr("data.source", "information_schema")
        }
        
        // Queued due to overload
        if load.avgQueuedLoad.Valid {
            queuedMetric := scopeMetrics.Metrics().AppendEmpty()
            queuedMetric.SetName("snowflake.warehouse.queries.queued.overload")
            queuedMetric.SetDescription("Average queries queued due to overload (REAL-TIME)")
            queuedMetric.SetUnit("{queries}")
            queuedGauge := queuedMetric.SetEmptyGauge()
            queuedDp := queuedGauge.DataPoints().AppendEmpty()
            queuedDp.SetTimestamp(now)
            queuedDp.SetDoubleValue(load.avgQueuedLoad.Float64)
            if load.warehouseName.Valid {
                queuedDp.Attributes().PutStr("warehouse.name", load.warehouseName.String)
            }
            queuedDp.Attributes().PutStr("data.source", "information_schema")
        }
        
        // Queued for provisioning
        if load.avgQueuedProvisioning.Valid {
            provMetric := scopeMetrics.Metrics().AppendEmpty()
            provMetric.SetName("snowflake.warehouse.queries.queued.provisioning")
            provMetric.SetDescription("Average queries queued for provisioning (REAL-TIME)")
            provMetric.SetUnit("{queries}")
            provGauge := provMetric.SetEmptyGauge()
            provDp := provGauge.DataPoints().AppendEmpty()
            provDp.SetTimestamp(now)
            provDp.SetDoubleValue(load.avgQueuedProvisioning.Float64)
            if load.warehouseName.Valid {
                provDp.Attributes().PutStr("warehouse.name", load.warehouseName.String)
            }
            provDp.Attributes().PutStr("data.source", "information_schema")
        }
        
        // Blocked queries
        if load.avgBlocked.Valid {
            blockedMetric := scopeMetrics.Metrics().AppendEmpty()
            blockedMetric.SetName("snowflake.warehouse.queries.blocked")
            blockedMetric.SetDescription("Average queries blocked (REAL-TIME)")
            blockedMetric.SetUnit("{queries}")
            blockedGauge := blockedMetric.SetEmptyGauge()
            blockedDp := blockedGauge.DataPoints().AppendEmpty()
            blockedDp.SetTimestamp(now)
            blockedDp.SetDoubleValue(load.avgBlocked.Float64)
            if load.warehouseName.Valid {
                blockedDp.Attributes().PutStr("warehouse.name", load.warehouseName.String)
            }
            blockedDp.Attributes().PutStr("data.source", "information_schema")
        }
    }
}

// ========== ACCOUNT_USAGE Metrics (Historical) ==========

func (s *snowflakeScraper) addQueryMetrics(scopeMetrics pmetric.ScopeMetrics, stats []queryStatRow, now pcommon.Timestamp) {
    for _, stat := range stats {
        // Query count
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.queries.count")
        metric.SetDescription("Number of queries executed (Historical)")
        metric.SetUnit("{queries}")
        gauge := metric.SetEmptyGauge()
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetIntValue(stat.queryCount)
        if stat.warehouseName.Valid {
            dp.Attributes().PutStr("warehouse.name", stat.warehouseName.String)
        }
        if stat.queryType.Valid {
            dp.Attributes().PutStr("query.type", stat.queryType.String)
        }
        if stat.executionStatus.Valid {
            dp.Attributes().PutStr("execution.status", stat.executionStatus.String)
        }
        dp.Attributes().PutStr("data.source", "account_usage")
        
        // Execution time
        if stat.avgExecutionTime.Valid {
            execMetric := scopeMetrics.Metrics().AppendEmpty()
            execMetric.SetName("snowflake.queries.execution.time")
            execMetric.SetDescription("Average query execution time (Historical)")
            execMetric.SetUnit("ms")
            execGauge := execMetric.SetEmptyGauge()
            execDp := execGauge.DataPoints().AppendEmpty()
            execDp.SetTimestamp(now)
            execDp.SetIntValue(int64(stat.avgExecutionTime.Float64))
            if stat.warehouseName.Valid {
                execDp.Attributes().PutStr("warehouse.name", stat.warehouseName.String)
            }
            if stat.queryType.Valid {
                execDp.Attributes().PutStr("query.type", stat.queryType.String)
            }
            execDp.Attributes().PutStr("data.source", "account_usage")
        }
        
        // Bytes scanned
        if stat.avgBytesScanned.Valid {
            bytesMetric := scopeMetrics.Metrics().AppendEmpty()
            bytesMetric.SetName("snowflake.queries.bytes.scanned")
            bytesMetric.SetDescription("Average bytes scanned per query (Historical)")
            bytesMetric.SetUnit("By")
            bytesGauge := bytesMetric.SetEmptyGauge()
            bytesDp := bytesGauge.DataPoints().AppendEmpty()
            bytesDp.SetTimestamp(now)
            bytesDp.SetIntValue(int64(stat.avgBytesScanned.Float64))
            if stat.warehouseName.Valid {
                bytesDp.Attributes().PutStr("warehouse.name", stat.warehouseName.String)
            }
            if stat.queryType.Valid {
                bytesDp.Attributes().PutStr("query.type", stat.queryType.String)
            }
            bytesDp.Attributes().PutStr("data.source", "account_usage")
        }
        
        // Bytes written
        if stat.avgBytesWritten.Valid {
            writtenMetric := scopeMetrics.Metrics().AppendEmpty()
            writtenMetric.SetName("snowflake.queries.bytes.written")
            writtenMetric.SetDescription("Average bytes written per query (Historical)")
            writtenMetric.SetUnit("By")
            writtenGauge := writtenMetric.SetEmptyGauge()
            writtenDp := writtenGauge.DataPoints().AppendEmpty()
            writtenDp.SetTimestamp(now)
            writtenDp.SetIntValue(int64(stat.avgBytesWritten.Float64))
            if stat.warehouseName.Valid {
                writtenDp.Attributes().PutStr("warehouse.name", stat.warehouseName.String)
            }
            if stat.queryType.Valid {
                writtenDp.Attributes().PutStr("query.type", stat.queryType.String)
            }
            writtenDp.Attributes().PutStr("data.source", "account_usage")
        }
        
        // Rows produced
        if stat.avgRowsProduced.Valid {
            rowsMetric := scopeMetrics.Metrics().AppendEmpty()
            rowsMetric.SetName("snowflake.queries.rows.produced")
            rowsMetric.SetDescription("Average rows produced per query (Historical)")
            rowsMetric.SetUnit("{rows}")
            rowsGauge := rowsMetric.SetEmptyGauge()
            rowsDp := rowsGauge.DataPoints().AppendEmpty()
            rowsDp.SetTimestamp(now)
            rowsDp.SetIntValue(int64(stat.avgRowsProduced.Float64))
            if stat.warehouseName.Valid {
                rowsDp.Attributes().PutStr("warehouse.name", stat.warehouseName.String)
            }
            if stat.queryType.Valid {
                rowsDp.Attributes().PutStr("query.type", stat.queryType.String)
            }
            rowsDp.Attributes().PutStr("data.source", "account_usage")
        }
        
        // Compilation time
        if stat.avgCompilationTime.Valid {
            compMetric := scopeMetrics.Metrics().AppendEmpty()
            compMetric.SetName("snowflake.queries.compilation.time")
            compMetric.SetDescription("Average query compilation time (Historical)")
            compMetric.SetUnit("ms")
            compGauge := compMetric.SetEmptyGauge()
            compDp := compGauge.DataPoints().AppendEmpty()
            compDp.SetTimestamp(now)
            compDp.SetIntValue(int64(stat.avgCompilationTime.Float64))
            if stat.warehouseName.Valid {
                compDp.Attributes().PutStr("warehouse.name", stat.warehouseName.String)
            }
            if stat.queryType.Valid {
                compDp.Attributes().PutStr("query.type", stat.queryType.String)
            }
            compDp.Attributes().PutStr("data.source", "account_usage")
        }
    }
}

func (s *snowflakeScraper) addCreditMetrics(scopeMetrics pmetric.ScopeMetrics, credits []creditUsageRow, now pcommon.Timestamp) {
    for _, credit := range credits {
        // Total credits
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.warehouse.credits.usage.total")
        metric.SetDescription("Warehouse credit usage total")
        metric.SetUnit("{credits}")
        gauge := metric.SetEmptyGauge()
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetDoubleValue(credit.totalCredits)
        if credit.warehouseName.Valid {
            dp.Attributes().PutStr("warehouse.name", credit.warehouseName.String)
        }
        dp.Attributes().PutStr("data.source", "account_usage")
        
        // Compute credits
        if credit.computeCredits.Valid {
            compMetric := scopeMetrics.Metrics().AppendEmpty()
            compMetric.SetName("snowflake.warehouse.credits.usage.compute")
            compMetric.SetDescription("Warehouse compute credit usage")
            compMetric.SetUnit("{credits}")
            compGauge := compMetric.SetEmptyGauge()
            compDp := compGauge.DataPoints().AppendEmpty()
            compDp.SetTimestamp(now)
            compDp.SetDoubleValue(credit.computeCredits.Float64)
            if credit.warehouseName.Valid {
                compDp.Attributes().PutStr("warehouse.name", credit.warehouseName.String)
            }
            compDp.Attributes().PutStr("data.source", "account_usage")
        }
        
        // Cloud services credits
        if credit.cloudServiceCredits.Valid {
            cloudMetric := scopeMetrics.Metrics().AppendEmpty()
            cloudMetric.SetName("snowflake.warehouse.credits.usage.cloud.services")
            cloudMetric.SetDescription("Warehouse cloud services credit usage")
            cloudMetric.SetUnit("{credits}")
            cloudGauge := cloudMetric.SetEmptyGauge()
            cloudDp := cloudGauge.DataPoints().AppendEmpty()
            cloudDp.SetTimestamp(now)
            cloudDp.SetDoubleValue(credit.cloudServiceCredits.Float64)
            if credit.warehouseName.Valid {
                cloudDp.Attributes().PutStr("warehouse.name", credit.warehouseName.String)
            }
            cloudDp.Attributes().PutStr("data.source", "account_usage")
        }
    }
}

func (s *snowflakeScraper) addStorageMetrics(scopeMetrics pmetric.ScopeMetrics, storage []storageUsageRow, now pcommon.Timestamp) {
    for _, st := range storage {
        // Total storage
        if st.totalStorageBytes.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.storage.bytes.total")
            metric.SetDescription("Total storage bytes")
            metric.SetUnit("By")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetIntValue(int64(st.totalStorageBytes.Float64))
            dp.Attributes().PutStr("data.source", "account_usage")
        }
        
        // Stage storage
        if st.stageBytes.Valid {
            stageMetric := scopeMetrics.Metrics().AppendEmpty()
            stageMetric.SetName("snowflake.storage.bytes.stage")
            stageMetric.SetDescription("Stage storage bytes")
            stageMetric.SetUnit("By")
            stageGauge := stageMetric.SetEmptyGauge()
            stageDp := stageGauge.DataPoints().AppendEmpty()
            stageDp.SetTimestamp(now)
            stageDp.SetIntValue(int64(st.stageBytes.Float64))
            stageDp.Attributes().PutStr("data.source", "account_usage")
        }
        
        // Failsafe storage
        if st.failsafeBytes.Valid {
            failsafeMetric := scopeMetrics.Metrics().AppendEmpty()
            failsafeMetric.SetName("snowflake.storage.bytes.failsafe")
            failsafeMetric.SetDescription("Failsafe storage bytes")
            failsafeMetric.SetUnit("By")
            failsafeGauge := failsafeMetric.SetEmptyGauge()
            failsafeDp := failsafeGauge.DataPoints().AppendEmpty()
            failsafeDp.SetTimestamp(now)
            failsafeDp.SetIntValue(int64(st.failsafeBytes.Float64))
            failsafeDp.Attributes().PutStr("data.source", "account_usage")
        }
    }
}

func (s *snowflakeScraper) addLoginMetrics(scopeMetrics pmetric.ScopeMetrics, logins []loginHistoryRow, now pcommon.Timestamp) {
    for _, login := range logins {
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.logins.count")
        metric.SetDescription("Number of login attempts")
        metric.SetUnit("{logins}")
        gauge := metric.SetEmptyGauge()
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetIntValue(login.loginCount)
        dp.Attributes().PutStr("is.success", login.isSuccess)
        if login.errorCode.Valid && login.errorCode.String != "" {
            dp.Attributes().PutStr("error.code", login.errorCode.String)
        }
        dp.Attributes().PutStr("data.source", "account_usage")
    }
}

func (s *snowflakeScraper) addPipeMetrics(scopeMetrics pmetric.ScopeMetrics, pipes []pipeUsageRow, now pcommon.Timestamp) {
    for _, pipe := range pipes {
        // Pipe credits
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.pipe.credits.usage")
        metric.SetDescription("Snowpipe credit usage")
        metric.SetUnit("{credits}")
        gauge := metric.SetEmptyGauge()
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetDoubleValue(pipe.totalCredits)
        if pipe.pipeName.Valid {
            dp.Attributes().PutStr("pipe.name", pipe.pipeName.String)
        }
        dp.Attributes().PutStr("data.source", "account_usage")
        
        // Bytes inserted
        if pipe.bytesInserted.Valid {
            bytesMetric := scopeMetrics.Metrics().AppendEmpty()
            bytesMetric.SetName("snowflake.pipe.bytes.inserted")
            bytesMetric.SetDescription("Bytes inserted via Snowpipe")
            bytesMetric.SetUnit("By")
            bytesGauge := bytesMetric.SetEmptyGauge()
            bytesDp := bytesGauge.DataPoints().AppendEmpty()
            bytesDp.SetTimestamp(now)
            bytesDp.SetIntValue(int64(pipe.bytesInserted.Float64))
            if pipe.pipeName.Valid {
                bytesDp.Attributes().PutStr("pipe.name", pipe.pipeName.String)
            }
            bytesDp.Attributes().PutStr("data.source", "account_usage")
        }
        
        // Files inserted
        if pipe.filesInserted.Valid {
            filesMetric := scopeMetrics.Metrics().AppendEmpty()
            filesMetric.SetName("snowflake.pipe.files.inserted")
            filesMetric.SetDescription("Files inserted via Snowpipe")
            filesMetric.SetUnit("{files}")
            filesGauge := filesMetric.SetEmptyGauge()
            filesDp := filesGauge.DataPoints().AppendEmpty()
            filesDp.SetTimestamp(now)
            filesDp.SetIntValue(pipe.filesInserted.Int64)
            if pipe.pipeName.Valid {
                filesDp.Attributes().PutStr("pipe.name", pipe.pipeName.String)
            }
            filesDp.Attributes().PutStr("data.source", "account_usage")
        }
    }
}

func (s *snowflakeScraper) addDatabaseStorageMetrics(scopeMetrics pmetric.ScopeMetrics, dbStorage []databaseStorageRow, now pcommon.Timestamp) {
    for _, db := range dbStorage {
        // Database storage
        if db.avgDatabaseBytes.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.database.storage.bytes")
            metric.SetDescription("Database storage bytes")
            metric.SetUnit("By")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetIntValue(int64(db.avgDatabaseBytes.Float64))
            if db.databaseName.Valid {
                dp.Attributes().PutStr("database.name", db.databaseName.String)
            }
            dp.Attributes().PutStr("data.source", "account_usage")
        }
        
        // Database failsafe
        if db.avgFailsafeBytes.Valid {
            failsafeMetric := scopeMetrics.Metrics().AppendEmpty()
            failsafeMetric.SetName("snowflake.database.failsafe.bytes")
            failsafeMetric.SetDescription("Database failsafe bytes")
            failsafeMetric.SetUnit("By")
            failsafeGauge := failsafeMetric.SetEmptyGauge()
            failsafeDp := failsafeGauge.DataPoints().AppendEmpty()
            failsafeDp.SetTimestamp(now)
            failsafeDp.SetIntValue(int64(db.avgFailsafeBytes.Float64))
            if db.databaseName.Valid {
                failsafeDp.Attributes().PutStr("database.name", db.databaseName.String)
            }
            failsafeDp.Attributes().PutStr("data.source", "account_usage")
        }
    }
}

func (s *snowflakeScraper) addTaskHistoryMetrics(scopeMetrics pmetric.ScopeMetrics, tasks []taskHistoryRow, now pcommon.Timestamp) {
    for _, task := range tasks {
        // Task execution count
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.tasks.executions.count")
        metric.SetDescription("Number of task executions")
        metric.SetUnit("{executions}")
        gauge := metric.SetEmptyGauge()
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetIntValue(task.executionCount)
        if task.databaseName.Valid {
            dp.Attributes().PutStr("database.name", task.databaseName.String)
        }
        if task.schemaName.Valid {
            dp.Attributes().PutStr("schema.name", task.schemaName.String)
        }
        if task.taskName.Valid {
            dp.Attributes().PutStr("task.name", task.taskName.String)
        }
        if task.state.Valid {
            dp.Attributes().PutStr("task.state", task.state.String)
        }
        dp.Attributes().PutStr("data.source", "account_usage")
        
        // Task execution time
        if task.avgExecutionTime.Valid {
            schedMetric := scopeMetrics.Metrics().AppendEmpty()
            schedMetric.SetName("snowflake.tasks.execution.time")
            schedMetric.SetDescription("Average task execution time")
            schedMetric.SetUnit("ms")
            schedGauge := schedMetric.SetEmptyGauge()
            schedDp := schedGauge.DataPoints().AppendEmpty()
            schedDp.SetTimestamp(now)
            schedDp.SetIntValue(int64(task.avgExecutionTime.Float64))
            if task.databaseName.Valid {
                schedDp.Attributes().PutStr("database.name", task.databaseName.String)
            }
            if task.schemaName.Valid {
                schedDp.Attributes().PutStr("schema.name", task.schemaName.String)
            }
            if task.taskName.Valid {
                schedDp.Attributes().PutStr("task.name", task.taskName.String)
            }
            schedDp.Attributes().PutStr("data.source", "account_usage")
        }
    }
}

func (s *snowflakeScraper) addReplicationMetrics(scopeMetrics pmetric.ScopeMetrics, replications []replicationUsageRow, now pcommon.Timestamp) {
    for _, repl := range replications {
        // Replication credits
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.replication.credits.usage")
        metric.SetDescription("Database replication credit usage")
        metric.SetUnit("{credits}")
        gauge := metric.SetEmptyGauge()
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetDoubleValue(repl.totalCredits)
        if repl.databaseName.Valid {
            dp.Attributes().PutStr("database.name", repl.databaseName.String)
        }
        dp.Attributes().PutStr("data.source", "account_usage")
        
        // Bytes transferred
        if repl.bytesTransferred.Valid {
            bytesMetric := scopeMetrics.Metrics().AppendEmpty()
            bytesMetric.SetName("snowflake.replication.bytes.transferred")
            bytesMetric.SetDescription("Bytes transferred via replication")
            bytesMetric.SetUnit("By")
            bytesGauge := bytesMetric.SetEmptyGauge()
            bytesDp := bytesGauge.DataPoints().AppendEmpty()
            bytesDp.SetTimestamp(now)
            bytesDp.SetIntValue(int64(repl.bytesTransferred.Float64))
            if repl.databaseName.Valid {
                bytesDp.Attributes().PutStr("database.name", repl.databaseName.String)
            }
            bytesDp.Attributes().PutStr("data.source", "account_usage")
        }
    }
}

func (s *snowflakeScraper) addAutoClusteringMetrics(scopeMetrics pmetric.ScopeMetrics, clusterings []autoClusteringRow, now pcommon.Timestamp) {
    for _, cluster := range clusterings {
        // Auto-clustering credits
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.clustering.auto.credits.usage")
        metric.SetDescription("Auto-clustering credit usage")
        metric.SetUnit("{credits}")
        gauge := metric.SetEmptyGauge()
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetDoubleValue(cluster.totalCredits)
        if cluster.databaseName.Valid {
            dp.Attributes().PutStr("database.name", cluster.databaseName.String)
        }
        if cluster.schemaName.Valid {
            dp.Attributes().PutStr("schema.name", cluster.schemaName.String)
        }
        if cluster.tableName.Valid {
            dp.Attributes().PutStr("table.name", cluster.tableName.String)
        }
        dp.Attributes().PutStr("data.source", "account_usage")
        
        // Bytes reclustered
        if cluster.bytesReclustered.Valid {
            bytesMetric := scopeMetrics.Metrics().AppendEmpty()
            bytesMetric.SetName("snowflake.clustering.auto.bytes.reclustered")
            bytesMetric.SetDescription("Bytes reclustered by auto-clustering")
            bytesMetric.SetUnit("By")
            bytesGauge := bytesMetric.SetEmptyGauge()
            bytesDp := bytesGauge.DataPoints().AppendEmpty()
            bytesDp.SetTimestamp(now)
            bytesDp.SetIntValue(int64(cluster.bytesReclustered.Float64))
            if cluster.databaseName.Valid {
                bytesDp.Attributes().PutStr("database.name", cluster.databaseName.String)
            }
            if cluster.schemaName.Valid {
                bytesDp.Attributes().PutStr("schema.name", cluster.schemaName.String)
            }
            if cluster.tableName.Valid {
                bytesDp.Attributes().PutStr("table.name", cluster.tableName.String)
            }
            bytesDp.Attributes().PutStr("data.source", "account_usage")
        }
        
        // Rows reclustered
        if cluster.rowsReclustered.Valid {
            rowsMetric := scopeMetrics.Metrics().AppendEmpty()
            rowsMetric.SetName("snowflake.clustering.auto.rows.reclustered")
            rowsMetric.SetDescription("Rows reclustered by auto-clustering")
            rowsMetric.SetUnit("{rows}")
            rowsGauge := rowsMetric.SetEmptyGauge()
            rowsDp := rowsGauge.DataPoints().AppendEmpty()
            rowsDp.SetTimestamp(now)
            rowsDp.SetIntValue(int64(cluster.rowsReclustered.Float64))
            if cluster.databaseName.Valid {
                rowsDp.Attributes().PutStr("database.name", cluster.databaseName.String)
            }
            if cluster.schemaName.Valid {
                rowsDp.Attributes().PutStr("schema.name", cluster.schemaName.String)
            }
            if cluster.tableName.Valid {
                rowsDp.Attributes().PutStr("table.name", cluster.tableName.String)
            }
            rowsDp.Attributes().PutStr("data.source", "account_usage")
        }
    }
}

// ========== EVENT TABLES Metrics (SECONDS-LEVEL LATENCY!) ==========

func (s *snowflakeScraper) addEventTableMetrics(scopeMetrics pmetric.ScopeMetrics, events []eventTableRow, now pcommon.Timestamp) {
    eventCounts := make(map[string]map[string]int64)
    
    for _, event := range events {
        if _, exists := eventCounts[event.eventType]; !exists {
            eventCounts[event.eventType] = make(map[string]int64)
        }
        
        severity := "INFO"
        if event.severity.Valid {
            severity = event.severity.String
        }
        eventCounts[event.eventType][severity]++
    }
    
    for eventType, severityCounts := range eventCounts {
        for severity, count := range severityCounts {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName(fmt.Sprintf("snowflake.events.%s.count", eventType))
            metric.SetDescription(fmt.Sprintf("Event table %s event count (REAL-TIME)", eventType))
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
    
    errorCount := int64(0)
    for _, event := range events {
        if event.errorMessage.Valid && event.errorMessage.String != "" {
            errorCount++
        }
    }
    
    if errorCount > 0 {
        errorMetric := scopeMetrics.Metrics().AppendEmpty()
        errorMetric.SetName("snowflake.events.errors.count")
        errorMetric.SetDescription("Event table error count (REAL-TIME)")
        errorMetric.SetUnit("{errors}")
        gauge := errorMetric.SetEmptyGauge()
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetIntValue(errorCount)
        dp.Attributes().PutStr("data.source", "event_table")
    }
}

// ========== ORGANIZATION Metrics (Multi-Account) ==========

func (s *snowflakeScraper) addOrgCreditMetrics(scopeMetrics pmetric.ScopeMetrics, orgCredits []orgCreditUsageRow, now pcommon.Timestamp) {
    for _, credit := range orgCredits {
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.organization.credits.usage")
        metric.SetDescription("Organization-level credit usage")
        metric.SetUnit("{credits}")
        gauge := metric.SetEmptyGauge()
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetDoubleValue(credit.totalCredits)
        if credit.organizationName.Valid {
            dp.Attributes().PutStr("organization.name", credit.organizationName.String)
        }
        if credit.accountName.Valid {
            dp.Attributes().PutStr("account.name", credit.accountName.String)
        }
        if credit.serviceType.Valid {
            dp.Attributes().PutStr("service.type", credit.serviceType.String)
        }
        dp.Attributes().PutStr("data.source", "organization_usage")
    }
}

func (s *snowflakeScraper) addOrgStorageMetrics(scopeMetrics pmetric.ScopeMetrics, orgStorage []orgStorageUsageRow, now pcommon.Timestamp) {
    for _, storage := range orgStorage {
        // Total storage
        if storage.avgStorageBytes.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.organization.storage.bytes.total")
            metric.SetDescription("Organization-level total storage")
            metric.SetUnit("By")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetIntValue(int64(storage.avgStorageBytes.Float64))
            if storage.organizationName.Valid {
                dp.Attributes().PutStr("organization.name", storage.organizationName.String)
            }
            if storage.accountName.Valid {
                dp.Attributes().PutStr("account.name", storage.accountName.String)
            }
            dp.Attributes().PutStr("data.source", "organization_usage")
        }
        
        // Stage storage
        if storage.avgStageBytes.Valid {
            stageMetric := scopeMetrics.Metrics().AppendEmpty()
            stageMetric.SetName("snowflake.organization.storage.bytes.stage")
            stageMetric.SetDescription("Organization-level stage storage")
            stageMetric.SetUnit("By")
            stageGauge := stageMetric.SetEmptyGauge()
            stageDp := stageGauge.DataPoints().AppendEmpty()
            stageDp.SetTimestamp(now)
            stageDp.SetIntValue(int64(storage.avgStageBytes.Float64))
            if storage.organizationName.Valid {
                stageDp.Attributes().PutStr("organization.name", storage.organizationName.String)
            }
            if storage.accountName.Valid {
                stageDp.Attributes().PutStr("account.name", storage.accountName.String)
            }
            stageDp.Attributes().PutStr("data.source", "organization_usage")
        }
        
        // Failsafe storage
        if storage.avgFailsafeBytes.Valid {
            failsafeMetric := scopeMetrics.Metrics().AppendEmpty()
            failsafeMetric.SetName("snowflake.organization.storage.bytes.failsafe")
            failsafeMetric.SetDescription("Organization-level failsafe storage")
            failsafeMetric.SetUnit("By")
            failsafeGauge := failsafeMetric.SetEmptyGauge()
            failsafeDp := failsafeGauge.DataPoints().AppendEmpty()
            failsafeDp.SetTimestamp(now)
            failsafeDp.SetIntValue(int64(storage.avgFailsafeBytes.Float64))
            if storage.organizationName.Valid {
                failsafeDp.Attributes().PutStr("organization.name", storage.organizationName.String)
            }
            if storage.accountName.Valid {
                failsafeDp.Attributes().PutStr("account.name", storage.accountName.String)
            }
            failsafeDp.Attributes().PutStr("data.source", "organization_usage")
        }
    }
}

func (s *snowflakeScraper) addOrgDataTransferMetrics(scopeMetrics pmetric.ScopeMetrics, transfers []orgDataTransferRow, now pcommon.Timestamp) {
    for _, transfer := range transfers {
        if transfer.totalBytesTransferred.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.organization.data.transfer.bytes")
            metric.SetDescription("Organization-level cross-account data transfer")
            metric.SetUnit("By")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetIntValue(int64(transfer.totalBytesTransferred.Float64))
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
        // Credits used
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
            dp.Attributes().PutStr("contract.number", fmt.Sprintf("%d", contract.contractNumber.Int64))
        }
        dp.Attributes().PutStr("data.source", "organization_usage")
        
        // Credits billed
        if contract.totalCreditsBilled.Valid {
            billedMetric := scopeMetrics.Metrics().AppendEmpty()
            billedMetric.SetName("snowflake.organization.contract.credits.billed")
            billedMetric.SetDescription("Organization contract credits billed")
            billedMetric.SetUnit("{credits}")
            billedGauge := billedMetric.SetEmptyGauge()
            billedDp := billedGauge.DataPoints().AppendEmpty()
            billedDp.SetTimestamp(now)
            billedDp.SetDoubleValue(contract.totalCreditsBilled.Float64)
            if contract.organizationName.Valid {
                billedDp.Attributes().PutStr("organization.name", contract.organizationName.String)
            }
            if contract.contractNumber.Valid {
                billedDp.Attributes().PutStr("contract.number", fmt.Sprintf("%d", contract.contractNumber.Int64))
            }
            billedDp.Attributes().PutStr("data.source", "organization_usage")
        }
    }
}

// ========== CUSTOM QUERIES ==========

func (s *snowflakeScraper) addCustomQueryMetrics(scopeMetrics pmetric.ScopeMetrics, result customQueryResult, now pcommon.Timestamp) {
    metricType := result.metricType
    if metricType == "" {
        metricType = "gauge"
    }
    
    var queryConfig *CustomQuery
    for i := range s.config.CustomQueries.Queries {
        if s.config.CustomQueries.Queries[i].Name == result.name {
            queryConfig = &s.config.CustomQueries.Queries[i]
            break
        }
    }
    
    if queryConfig == nil {
        s.logger.Warn("Custom query config not found", zap.String("query_name", result.name))
        return
    }
    
    for _, row := range result.rows {
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName(fmt.Sprintf("snowflake.custom.%s", result.name))
        metric.SetDescription(fmt.Sprintf("Custom query: %s", result.name))
        metric.SetUnit("1")
        
        valueInterface, exists := row[queryConfig.ValueColumn]
        if !exists {
            s.logger.Warn("Value column not found in query result",
                zap.String("query_name", result.name),
                zap.String("value_column", queryConfig.ValueColumn))
            continue
        }
        
        gauge := metric.SetEmptyGauge()
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        
        var floatValue float64
        switch v := valueInterface.(type) {
        case float64:
            floatValue = v
        case float32:
            floatValue = float64(v)
        case int64:
            floatValue = float64(v)
        case int32:
            floatValue = float64(v)
        case int:
            floatValue = float64(v)
        case string:
            parsed, err := strconv.ParseFloat(v, 64)
            if err != nil {
                s.logger.Warn("Failed to parse string value to float",
                    zap.String("query_name", result.name),
                    zap.String("value", v),
                    zap.Error(err))
                continue
            }
            floatValue = parsed
        case []byte:
            parsed, err := strconv.ParseFloat(string(v), 64)
            if err != nil {
                s.logger.Warn("Failed to parse byte value to float",
                    zap.String("query_name", result.name),
                    zap.Error(err))
                continue
            }
            floatValue = parsed
        default:
            s.logger.Warn("Unsupported value type in custom query",
                zap.String("query_name", result.name),
                zap.String("type", fmt.Sprintf("%T", v)),
                zap.Any("value", v))
            continue
        }
        
        dp.SetDoubleValue(floatValue)
        
        for _, labelCol := range queryConfig.LabelColumns {
            if labelValue, exists := row[labelCol]; exists && labelValue != nil {
                dp.Attributes().PutStr(labelCol, fmt.Sprintf("%v", labelValue))
            }
        }
        
        dp.Attributes().PutStr("data.source", "custom_query")
    }
}
