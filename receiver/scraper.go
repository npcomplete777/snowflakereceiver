package snowflakereceiver

import (
    "context"
    "time"
    
    "go.opentelemetry.io/collector/pdata/pcommon"
    "go.opentelemetry.io/collector/pdata/pmetric"
    "go.opentelemetry.io/collector/receiver"
    "go.uber.org/zap"
)

type snowflakeScraper struct {
    logger *zap.Logger
    config *Config
    client *snowflakeClient
}

func newSnowflakeScraper(settings receiver.Settings, config *Config) (*snowflakeScraper, error) {
    client, err := newSnowflakeClient(settings.Logger, config)
    if err != nil {
        return nil, err
    }
    
    return &snowflakeScraper{
        logger: settings.Logger,
        config: config,
        client: client,
    }, nil
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
    
    // INFORMATION_SCHEMA metrics (REAL-TIME!)
    if s.config.Metrics.CurrentQueries && len(metrics.currentQueries) > 0 {
        s.addCurrentQueryMetrics(scopeMetrics, metrics.currentQueries, now)
    }
    
    if s.config.Metrics.WarehouseLoad && len(metrics.warehouseLoad) > 0 {
        s.addWarehouseLoadMetrics(scopeMetrics, metrics.warehouseLoad, now)
    }
    
    // ACCOUNT_USAGE metrics (Historical)
    if s.config.Metrics.QueryHistory && len(metrics.queryStats) > 0 {
        s.addQueryMetrics(scopeMetrics, metrics.queryStats, now)
    }
    
    if s.config.Metrics.CreditUsage && len(metrics.creditUsage) > 0 {
        s.addCreditMetrics(scopeMetrics, metrics.creditUsage, now)
    }
    
    if s.config.Metrics.StorageMetrics && len(metrics.storageUsage) > 0 {
        s.addStorageMetrics(scopeMetrics, metrics.storageUsage, now)
    }
    
    if s.config.Metrics.LoginHistory && len(metrics.loginHistory) > 0 {
        s.addLoginMetrics(scopeMetrics, metrics.loginHistory, now)
    }
    
    if s.config.Metrics.DataPipeline && len(metrics.pipeUsage) > 0 {
        s.addPipeMetrics(scopeMetrics, metrics.pipeUsage, now)
    }
    
    if s.config.Metrics.DatabaseStorage && len(metrics.databaseStorage) > 0 {
        s.addDatabaseStorageMetrics(scopeMetrics, metrics.databaseStorage, now)
    }
    
    if s.config.Metrics.TaskHistory && len(metrics.taskHistory) > 0 {
        s.addTaskHistoryMetrics(scopeMetrics, metrics.taskHistory, now)
    }
    
    if s.config.Metrics.ReplicationUsage && len(metrics.replicationUsage) > 0 {
        s.addReplicationMetrics(scopeMetrics, metrics.replicationUsage, now)
    }
    
    if s.config.Metrics.AutoClusteringHistory && len(metrics.autoClusteringHistory) > 0 {
        s.addAutoClusteringMetrics(scopeMetrics, metrics.autoClusteringHistory, now)
    }
    
    return md
}

// ========== INFORMATION_SCHEMA Metrics (REAL-TIME) ==========

func (s *snowflakeScraper) addCurrentQueryMetrics(scopeMetrics pmetric.ScopeMetrics, queries []currentQueryRow, now pcommon.Timestamp) {
    for _, query := range queries {
        // Real-time query count
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.current_queries.count")
        metric.SetDescription("Current query count (last 5 minutes, REAL-TIME)")
        metric.SetUnit("{queries}")
        gauge := metric.SetEmptyGauge()
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetIntValue(query.queryCount)
        if query.warehouseName.Valid {
            dp.Attributes().PutStr("warehouse_name", query.warehouseName.String)
        }
        if query.queryType.Valid {
            dp.Attributes().PutStr("query_type", query.queryType.String)
        }
        if query.executionStatus.Valid {
            dp.Attributes().PutStr("execution_status", query.executionStatus.String)
        }
        dp.Attributes().PutStr("data_source", "INFORMATION_SCHEMA")
        
        // Real-time execution time
        if query.avgExecutionTime.Valid {
            execMetric := scopeMetrics.Metrics().AppendEmpty()
            execMetric.SetName("snowflake.current_queries.execution_time")
            execMetric.SetDescription("Current query execution time (REAL-TIME)")
            execMetric.SetUnit("ms")
            execGauge := execMetric.SetEmptyGauge()
            execDp := execGauge.DataPoints().AppendEmpty()
            execDp.SetTimestamp(now)
            execDp.SetIntValue(int64(query.avgExecutionTime.Float64))
            if query.warehouseName.Valid {
                execDp.Attributes().PutStr("warehouse_name", query.warehouseName.String)
            }
            if query.queryType.Valid {
                execDp.Attributes().PutStr("query_type", query.queryType.String)
            }
            execDp.Attributes().PutStr("data_source", "INFORMATION_SCHEMA")
        }
        
        // Real-time bytes scanned
        if query.avgBytesScanned.Valid {
            bytesMetric := scopeMetrics.Metrics().AppendEmpty()
            bytesMetric.SetName("snowflake.current_queries.bytes_scanned")
            bytesMetric.SetDescription("Current query bytes scanned (REAL-TIME)")
            bytesMetric.SetUnit("By")
            bytesGauge := bytesMetric.SetEmptyGauge()
            bytesDp := bytesGauge.DataPoints().AppendEmpty()
            bytesDp.SetTimestamp(now)
            bytesDp.SetIntValue(int64(query.avgBytesScanned.Float64))
            if query.warehouseName.Valid {
                bytesDp.Attributes().PutStr("warehouse_name", query.warehouseName.String)
            }
            if query.queryType.Valid {
                bytesDp.Attributes().PutStr("query_type", query.queryType.String)
            }
            bytesDp.Attributes().PutStr("data_source", "INFORMATION_SCHEMA")
        }
    }
}

func (s *snowflakeScraper) addWarehouseLoadMetrics(scopeMetrics pmetric.ScopeMetrics, loads []warehouseLoadRow, now pcommon.Timestamp) {
    for _, load := range loads {
        // Running queries
        if load.avgRunning.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.warehouse.queries_running")
            metric.SetDescription("Average queries running (REAL-TIME)")
            metric.SetUnit("{queries}")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetDoubleValue(load.avgRunning.Float64)
            if load.warehouseName.Valid {
                dp.Attributes().PutStr("warehouse_name", load.warehouseName.String)
            }
            dp.Attributes().PutStr("data_source", "INFORMATION_SCHEMA")
        }
        
        // Queued for overload
        if load.avgQueuedLoad.Valid {
            queuedMetric := scopeMetrics.Metrics().AppendEmpty()
            queuedMetric.SetName("snowflake.warehouse.queries_queued_overload")
            queuedMetric.SetDescription("Average queries queued due to overload (REAL-TIME)")
            queuedMetric.SetUnit("{queries}")
            queuedGauge := queuedMetric.SetEmptyGauge()
            queuedDp := queuedGauge.DataPoints().AppendEmpty()
            queuedDp.SetTimestamp(now)
            queuedDp.SetDoubleValue(load.avgQueuedLoad.Float64)
            if load.warehouseName.Valid {
                queuedDp.Attributes().PutStr("warehouse_name", load.warehouseName.String)
            }
            queuedDp.Attributes().PutStr("data_source", "INFORMATION_SCHEMA")
        }
        
        // Queued for provisioning
        if load.avgQueuedProvisioning.Valid {
            provMetric := scopeMetrics.Metrics().AppendEmpty()
            provMetric.SetName("snowflake.warehouse.queries_queued_provisioning")
            provMetric.SetDescription("Average queries queued for provisioning (REAL-TIME)")
            provMetric.SetUnit("{queries}")
            provGauge := provMetric.SetEmptyGauge()
            provDp := provGauge.DataPoints().AppendEmpty()
            provDp.SetTimestamp(now)
            provDp.SetDoubleValue(load.avgQueuedProvisioning.Float64)
            if load.warehouseName.Valid {
                provDp.Attributes().PutStr("warehouse_name", load.warehouseName.String)
            }
            provDp.Attributes().PutStr("data_source", "INFORMATION_SCHEMA")
        }
        
        // Blocked queries
        if load.avgBlocked.Valid {
            blockedMetric := scopeMetrics.Metrics().AppendEmpty()
            blockedMetric.SetName("snowflake.warehouse.queries_blocked")
            blockedMetric.SetDescription("Average queries blocked (REAL-TIME)")
            blockedMetric.SetUnit("{queries}")
            blockedGauge := blockedMetric.SetEmptyGauge()
            blockedDp := blockedGauge.DataPoints().AppendEmpty()
            blockedDp.SetTimestamp(now)
            blockedDp.SetDoubleValue(load.avgBlocked.Float64)
            if load.warehouseName.Valid {
                blockedDp.Attributes().PutStr("warehouse_name", load.warehouseName.String)
            }
            blockedDp.Attributes().PutStr("data_source", "INFORMATION_SCHEMA")
        }
    }
}

// ========== ACCOUNT_USAGE Metrics (Historical - 45min-3hr latency) ==========

func (s *snowflakeScraper) addQueryMetrics(scopeMetrics pmetric.ScopeMetrics, stats []queryStatRow, now pcommon.Timestamp) {
    for _, stat := range stats {
        // Query count
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.query.count")
        metric.SetDescription("Number of queries executed (Historical)")
        metric.SetUnit("{queries}")
        gauge := metric.SetEmptyGauge()
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetIntValue(stat.queryCount)
        if stat.warehouseName.Valid {
            dp.Attributes().PutStr("warehouse_name", stat.warehouseName.String)
        }
        if stat.queryType.Valid {
            dp.Attributes().PutStr("query_type", stat.queryType.String)
        }
        if stat.executionStatus.Valid {
            dp.Attributes().PutStr("execution_status", stat.executionStatus.String)
        }
        dp.Attributes().PutStr("data_source", "ACCOUNT_USAGE")
        
        // Execution time
        if stat.avgExecutionTime.Valid {
            execMetric := scopeMetrics.Metrics().AppendEmpty()
            execMetric.SetName("snowflake.query.execution_time")
            execMetric.SetDescription("Average query execution time (Historical)")
            execMetric.SetUnit("ms")
            execGauge := execMetric.SetEmptyGauge()
            execDp := execGauge.DataPoints().AppendEmpty()
            execDp.SetTimestamp(now)
            execDp.SetIntValue(int64(stat.avgExecutionTime.Float64))
            if stat.warehouseName.Valid {
                execDp.Attributes().PutStr("warehouse_name", stat.warehouseName.String)
            }
            if stat.queryType.Valid {
                execDp.Attributes().PutStr("query_type", stat.queryType.String)
            }
            execDp.Attributes().PutStr("data_source", "ACCOUNT_USAGE")
        }
        
        // Bytes scanned
        if stat.avgBytesScanned.Valid {
            bytesMetric := scopeMetrics.Metrics().AppendEmpty()
            bytesMetric.SetName("snowflake.query.bytes_scanned")
            bytesMetric.SetDescription("Average bytes scanned per query (Historical)")
            bytesMetric.SetUnit("By")
            bytesGauge := bytesMetric.SetEmptyGauge()
            bytesDp := bytesGauge.DataPoints().AppendEmpty()
            bytesDp.SetTimestamp(now)
            bytesDp.SetIntValue(int64(stat.avgBytesScanned.Float64))
            if stat.warehouseName.Valid {
                bytesDp.Attributes().PutStr("warehouse_name", stat.warehouseName.String)
            }
            if stat.queryType.Valid {
                bytesDp.Attributes().PutStr("query_type", stat.queryType.String)
            }
            bytesDp.Attributes().PutStr("data_source", "ACCOUNT_USAGE")
        }
        
        // Bytes written
        if stat.avgBytesWritten.Valid {
            writtenMetric := scopeMetrics.Metrics().AppendEmpty()
            writtenMetric.SetName("snowflake.query.bytes_written")
            writtenMetric.SetDescription("Average bytes written per query (Historical)")
            writtenMetric.SetUnit("By")
            writtenGauge := writtenMetric.SetEmptyGauge()
            writtenDp := writtenGauge.DataPoints().AppendEmpty()
            writtenDp.SetTimestamp(now)
            writtenDp.SetIntValue(int64(stat.avgBytesWritten.Float64))
            if stat.warehouseName.Valid {
                writtenDp.Attributes().PutStr("warehouse_name", stat.warehouseName.String)
            }
            if stat.queryType.Valid {
                writtenDp.Attributes().PutStr("query_type", stat.queryType.String)
            }
            writtenDp.Attributes().PutStr("data_source", "ACCOUNT_USAGE")
        }
        
        // Rows produced
        if stat.avgRowsProduced.Valid {
            rowsMetric := scopeMetrics.Metrics().AppendEmpty()
            rowsMetric.SetName("snowflake.query.rows_produced")
            rowsMetric.SetDescription("Average rows produced per query (Historical)")
            rowsMetric.SetUnit("{rows}")
            rowsGauge := rowsMetric.SetEmptyGauge()
            rowsDp := rowsGauge.DataPoints().AppendEmpty()
            rowsDp.SetTimestamp(now)
            rowsDp.SetIntValue(int64(stat.avgRowsProduced.Float64))
            if stat.warehouseName.Valid {
                rowsDp.Attributes().PutStr("warehouse_name", stat.warehouseName.String)
            }
            if stat.queryType.Valid {
                rowsDp.Attributes().PutStr("query_type", stat.queryType.String)
            }
            rowsDp.Attributes().PutStr("data_source", "ACCOUNT_USAGE")
        }
        
        // Compilation time
        if stat.avgCompilationTime.Valid {
            compMetric := scopeMetrics.Metrics().AppendEmpty()
            compMetric.SetName("snowflake.query.compilation_time")
            compMetric.SetDescription("Average query compilation time (Historical)")
            compMetric.SetUnit("ms")
            compGauge := compMetric.SetEmptyGauge()
            compDp := compGauge.DataPoints().AppendEmpty()
            compDp.SetTimestamp(now)
            compDp.SetIntValue(int64(stat.avgCompilationTime.Float64))
            if stat.warehouseName.Valid {
                compDp.Attributes().PutStr("warehouse_name", stat.warehouseName.String)
            }
            if stat.queryType.Valid {
                compDp.Attributes().PutStr("query_type", stat.queryType.String)
            }
            compDp.Attributes().PutStr("data_source", "ACCOUNT_USAGE")
        }
    }
}

func (s *snowflakeScraper) addCreditMetrics(scopeMetrics pmetric.ScopeMetrics, credits []creditUsageRow, now pcommon.Timestamp) {
    for _, credit := range credits {
        // Total credits
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.warehouse.credit_usage")
        metric.SetDescription("Warehouse credit usage")
        metric.SetUnit("{credits}")
        gauge := metric.SetEmptyGauge()
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetDoubleValue(credit.totalCredits)
        if credit.warehouseName.Valid {
            dp.Attributes().PutStr("warehouse_name", credit.warehouseName.String)
        }
        dp.Attributes().PutStr("data_source", "ACCOUNT_USAGE")
        
        // Compute credits
        if credit.computeCredits.Valid {
            compMetric := scopeMetrics.Metrics().AppendEmpty()
            compMetric.SetName("snowflake.warehouse.credit_usage.compute")
            compMetric.SetDescription("Warehouse compute credit usage")
            compMetric.SetUnit("{credits}")
            compGauge := compMetric.SetEmptyGauge()
            compDp := compGauge.DataPoints().AppendEmpty()
            compDp.SetTimestamp(now)
            compDp.SetDoubleValue(credit.computeCredits.Float64)
            if credit.warehouseName.Valid {
                compDp.Attributes().PutStr("warehouse_name", credit.warehouseName.String)
            }
            compDp.Attributes().PutStr("data_source", "ACCOUNT_USAGE")
        }
        
        // Cloud service credits
        if credit.cloudServiceCredits.Valid {
            cloudMetric := scopeMetrics.Metrics().AppendEmpty()
            cloudMetric.SetName("snowflake.warehouse.credit_usage.cloud_services")
            cloudMetric.SetDescription("Warehouse cloud services credit usage")
            cloudMetric.SetUnit("{credits}")
            cloudGauge := cloudMetric.SetEmptyGauge()
            cloudDp := cloudGauge.DataPoints().AppendEmpty()
            cloudDp.SetTimestamp(now)
            cloudDp.SetDoubleValue(credit.cloudServiceCredits.Float64)
            if credit.warehouseName.Valid {
                cloudDp.Attributes().PutStr("warehouse_name", credit.warehouseName.String)
            }
            cloudDp.Attributes().PutStr("data_source", "ACCOUNT_USAGE")
        }
    }
}

func (s *snowflakeScraper) addStorageMetrics(scopeMetrics pmetric.ScopeMetrics, storage []storageUsageRow, now pcommon.Timestamp) {
    for _, st := range storage {
        // Total storage
        if st.totalStorageBytes.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.storage.total")
            metric.SetDescription("Total storage bytes")
            metric.SetUnit("By")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetIntValue(int64(st.totalStorageBytes.Float64))
            dp.Attributes().PutStr("data_source", "ACCOUNT_USAGE")
        }
        
        // Stage bytes
        if st.stageBytes.Valid {
            stageMetric := scopeMetrics.Metrics().AppendEmpty()
            stageMetric.SetName("snowflake.storage.stage")
            stageMetric.SetDescription("Stage storage bytes")
            stageMetric.SetUnit("By")
            stageGauge := stageMetric.SetEmptyGauge()
            stageDp := stageGauge.DataPoints().AppendEmpty()
            stageDp.SetTimestamp(now)
            stageDp.SetIntValue(int64(st.stageBytes.Float64))
            stageDp.Attributes().PutStr("data_source", "ACCOUNT_USAGE")
        }
        
        // Failsafe bytes
        if st.failsafeBytes.Valid {
            failsafeMetric := scopeMetrics.Metrics().AppendEmpty()
            failsafeMetric.SetName("snowflake.storage.failsafe")
            failsafeMetric.SetDescription("Failsafe storage bytes")
            failsafeMetric.SetUnit("By")
            failsafeGauge := failsafeMetric.SetEmptyGauge()
            failsafeDp := failsafeGauge.DataPoints().AppendEmpty()
            failsafeDp.SetTimestamp(now)
            failsafeDp.SetIntValue(int64(st.failsafeBytes.Float64))
            failsafeDp.Attributes().PutStr("data_source", "ACCOUNT_USAGE")
        }
    }
}

func (s *snowflakeScraper) addLoginMetrics(scopeMetrics pmetric.ScopeMetrics, logins []loginHistoryRow, now pcommon.Timestamp) {
    for _, login := range logins {
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.login.count")
        metric.SetDescription("Number of login attempts")
        metric.SetUnit("{logins}")
        gauge := metric.SetEmptyGauge()
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetIntValue(login.loginCount)
        dp.Attributes().PutStr("is_success", login.isSuccess)
        if login.errorCode.Valid && login.errorCode.String != "" {
            dp.Attributes().PutStr("error_code", login.errorCode.String)
        }
        dp.Attributes().PutStr("data_source", "ACCOUNT_USAGE")
    }
}

func (s *snowflakeScraper) addPipeMetrics(scopeMetrics pmetric.ScopeMetrics, pipes []pipeUsageRow, now pcommon.Timestamp) {
    for _, pipe := range pipes {
        // Pipe credits
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.pipe.credit_usage")
        metric.SetDescription("Snowpipe credit usage")
        metric.SetUnit("{credits}")
        gauge := metric.SetEmptyGauge()
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetDoubleValue(pipe.totalCredits)
        if pipe.pipeName.Valid {
            dp.Attributes().PutStr("pipe_name", pipe.pipeName.String)
        }
        dp.Attributes().PutStr("data_source", "ACCOUNT_USAGE")
        
        // Bytes inserted
        if pipe.bytesInserted.Valid {
            bytesMetric := scopeMetrics.Metrics().AppendEmpty()
            bytesMetric.SetName("snowflake.pipe.bytes_inserted")
            bytesMetric.SetDescription("Bytes inserted via Snowpipe")
            bytesMetric.SetUnit("By")
            bytesGauge := bytesMetric.SetEmptyGauge()
            bytesDp := bytesGauge.DataPoints().AppendEmpty()
            bytesDp.SetTimestamp(now)
            bytesDp.SetIntValue(int64(pipe.bytesInserted.Float64))
            if pipe.pipeName.Valid {
                bytesDp.Attributes().PutStr("pipe_name", pipe.pipeName.String)
            }
            bytesDp.Attributes().PutStr("data_source", "ACCOUNT_USAGE")
        }
        
        // Files inserted
        if pipe.filesInserted.Valid {
            filesMetric := scopeMetrics.Metrics().AppendEmpty()
            filesMetric.SetName("snowflake.pipe.files_inserted")
            filesMetric.SetDescription("Files inserted via Snowpipe")
            filesMetric.SetUnit("{files}")
            filesGauge := filesMetric.SetEmptyGauge()
            filesDp := filesGauge.DataPoints().AppendEmpty()
            filesDp.SetTimestamp(now)
            filesDp.SetIntValue(pipe.filesInserted.Int64)
            if pipe.pipeName.Valid {
                filesDp.Attributes().PutStr("pipe_name", pipe.pipeName.String)
            }
            filesDp.Attributes().PutStr("data_source", "ACCOUNT_USAGE")
        }
    }
}

func (s *snowflakeScraper) addDatabaseStorageMetrics(scopeMetrics pmetric.ScopeMetrics, dbStorage []databaseStorageRow, now pcommon.Timestamp) {
    for _, db := range dbStorage {
        // Database storage
        if db.avgDatabaseBytes.Valid {
            metric := scopeMetrics.Metrics().AppendEmpty()
            metric.SetName("snowflake.database.storage")
            metric.SetDescription("Database storage bytes")
            metric.SetUnit("By")
            gauge := metric.SetEmptyGauge()
            dp := gauge.DataPoints().AppendEmpty()
            dp.SetTimestamp(now)
            dp.SetIntValue(int64(db.avgDatabaseBytes.Float64))
            if db.databaseName.Valid {
                dp.Attributes().PutStr("database_name", db.databaseName.String)
            }
            dp.Attributes().PutStr("data_source", "ACCOUNT_USAGE")
        }
        
        // Database failsafe
        if db.avgFailsafeBytes.Valid {
            failsafeMetric := scopeMetrics.Metrics().AppendEmpty()
            failsafeMetric.SetName("snowflake.database.failsafe")
            failsafeMetric.SetDescription("Database failsafe bytes")
            failsafeMetric.SetUnit("By")
            failsafeGauge := failsafeMetric.SetEmptyGauge()
            failsafeDp := failsafeGauge.DataPoints().AppendEmpty()
            failsafeDp.SetTimestamp(now)
            failsafeDp.SetIntValue(int64(db.avgFailsafeBytes.Float64))
            if db.databaseName.Valid {
                failsafeDp.Attributes().PutStr("database_name", db.databaseName.String)
            }
            failsafeDp.Attributes().PutStr("data_source", "ACCOUNT_USAGE")
        }
    }
}

func (s *snowflakeScraper) addTaskHistoryMetrics(scopeMetrics pmetric.ScopeMetrics, tasks []taskHistoryRow, now pcommon.Timestamp) {
    for _, task := range tasks {
        // Task execution count
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.task.execution_count")
        metric.SetDescription("Number of task executions")
        metric.SetUnit("{executions}")
        gauge := metric.SetEmptyGauge()
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetIntValue(task.executionCount)
        if task.databaseName.Valid {
            dp.Attributes().PutStr("database_name", task.databaseName.String)
        }
        if task.schemaName.Valid {
            dp.Attributes().PutStr("schema_name", task.schemaName.String)
        }
        if task.taskName.Valid {
            dp.Attributes().PutStr("task_name", task.taskName.String)
        }
        if task.state.Valid {
            dp.Attributes().PutStr("state", task.state.String)
        }
        dp.Attributes().PutStr("data_source", "ACCOUNT_USAGE")
        
        // Average scheduled time
        if task.avgScheduledTime.Valid {
            schedMetric := scopeMetrics.Metrics().AppendEmpty()
            schedMetric.SetName("snowflake.task.scheduled_time")
            schedMetric.SetDescription("Average task scheduled time")
            schedMetric.SetUnit("ms")
            schedGauge := schedMetric.SetEmptyGauge()
            schedDp := schedGauge.DataPoints().AppendEmpty()
            schedDp.SetTimestamp(now)
            schedDp.SetIntValue(int64(task.avgScheduledTime.Float64))
            if task.databaseName.Valid {
                schedDp.Attributes().PutStr("database_name", task.databaseName.String)
            }
            if task.schemaName.Valid {
                schedDp.Attributes().PutStr("schema_name", task.schemaName.String)
            }
            if task.taskName.Valid {
                schedDp.Attributes().PutStr("task_name", task.taskName.String)
            }
            schedDp.Attributes().PutStr("data_source", "ACCOUNT_USAGE")
        }
    }
}

func (s *snowflakeScraper) addReplicationMetrics(scopeMetrics pmetric.ScopeMetrics, replications []replicationUsageRow, now pcommon.Timestamp) {
    for _, repl := range replications {
        // Replication credits
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.replication.credit_usage")
        metric.SetDescription("Database replication credit usage")
        metric.SetUnit("{credits}")
        gauge := metric.SetEmptyGauge()
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetDoubleValue(repl.totalCredits)
        if repl.databaseName.Valid {
            dp.Attributes().PutStr("database_name", repl.databaseName.String)
        }
        dp.Attributes().PutStr("data_source", "ACCOUNT_USAGE")
        
        // Bytes transferred
        if repl.bytesTransferred.Valid {
            bytesMetric := scopeMetrics.Metrics().AppendEmpty()
            bytesMetric.SetName("snowflake.replication.bytes_transferred")
            bytesMetric.SetDescription("Bytes transferred via replication")
            bytesMetric.SetUnit("By")
            bytesGauge := bytesMetric.SetEmptyGauge()
            bytesDp := bytesGauge.DataPoints().AppendEmpty()
            bytesDp.SetTimestamp(now)
            bytesDp.SetIntValue(int64(repl.bytesTransferred.Float64))
            if repl.databaseName.Valid {
                bytesDp.Attributes().PutStr("database_name", repl.databaseName.String)
            }
            bytesDp.Attributes().PutStr("data_source", "ACCOUNT_USAGE")
        }
    }
}

func (s *snowflakeScraper) addAutoClusteringMetrics(scopeMetrics pmetric.ScopeMetrics, clusterings []autoClusteringRow, now pcommon.Timestamp) {
    for _, cluster := range clusterings {
        // Auto-clustering credits
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.auto_clustering.credit_usage")
        metric.SetDescription("Auto-clustering credit usage")
        metric.SetUnit("{credits}")
        gauge := metric.SetEmptyGauge()
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetTimestamp(now)
        dp.SetDoubleValue(cluster.totalCredits)
        if cluster.databaseName.Valid {
            dp.Attributes().PutStr("database_name", cluster.databaseName.String)
        }
        if cluster.schemaName.Valid {
            dp.Attributes().PutStr("schema_name", cluster.schemaName.String)
        }
        if cluster.tableName.Valid {
            dp.Attributes().PutStr("table_name", cluster.tableName.String)
        }
        dp.Attributes().PutStr("data_source", "ACCOUNT_USAGE")
        
        // Bytes reclustered
        if cluster.bytesReclustered.Valid {
            bytesMetric := scopeMetrics.Metrics().AppendEmpty()
            bytesMetric.SetName("snowflake.auto_clustering.bytes_reclustered")
            bytesMetric.SetDescription("Bytes reclustered by auto-clustering")
            bytesMetric.SetUnit("By")
            bytesGauge := bytesMetric.SetEmptyGauge()
            bytesDp := bytesGauge.DataPoints().AppendEmpty()
            bytesDp.SetTimestamp(now)
            bytesDp.SetIntValue(int64(cluster.bytesReclustered.Float64))
            if cluster.databaseName.Valid {
                bytesDp.Attributes().PutStr("database_name", cluster.databaseName.String)
            }
            if cluster.schemaName.Valid {
                bytesDp.Attributes().PutStr("schema_name", cluster.schemaName.String)
            }
            if cluster.tableName.Valid {
                bytesDp.Attributes().PutStr("table_name", cluster.tableName.String)
            }
            bytesDp.Attributes().PutStr("data_source", "ACCOUNT_USAGE")
        }
        
        // Rows reclustered
        if cluster.rowsReclustered.Valid {
            rowsMetric := scopeMetrics.Metrics().AppendEmpty()
            rowsMetric.SetName("snowflake.auto_clustering.rows_reclustered")
            rowsMetric.SetDescription("Rows reclustered by auto-clustering")
            rowsMetric.SetUnit("{rows}")
            rowsGauge := rowsMetric.SetEmptyGauge()
            rowsDp := rowsGauge.DataPoints().AppendEmpty()
            rowsDp.SetTimestamp(now)
            rowsDp.SetIntValue(int64(cluster.rowsReclustered.Float64))
            if cluster.databaseName.Valid {
                rowsDp.Attributes().PutStr("database_name", cluster.databaseName.String)
            }
            if cluster.schemaName.Valid {
                rowsDp.Attributes().PutStr("schema_name", cluster.schemaName.String)
            }
            if cluster.tableName.Valid {
                rowsDp.Attributes().PutStr("table_name", cluster.tableName.String)
            }
            rowsDp.Attributes().PutStr("data_source", "ACCOUNT_USAGE")
        }
    }
}
