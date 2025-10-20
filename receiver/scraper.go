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
    
    // Add all metric categories
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
    
    return md
}

func (s *snowflakeScraper) addQueryMetrics(scopeMetrics pmetric.ScopeMetrics, stats []queryStatRow, now pcommon.Timestamp) {
    for _, stat := range stats {
        // Query count
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("snowflake.query.count")
        metric.SetDescription("Number of queries executed")
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
        
        // Execution time
        if stat.avgExecutionTime.Valid {
            execMetric := scopeMetrics.Metrics().AppendEmpty()
            execMetric.SetName("snowflake.query.execution_time")
            execMetric.SetDescription("Average query execution time")
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
        }
        
        // Bytes scanned
        if stat.avgBytesScanned.Valid {
            bytesMetric := scopeMetrics.Metrics().AppendEmpty()
            bytesMetric.SetName("snowflake.query.bytes_scanned")
            bytesMetric.SetDescription("Average bytes scanned per query")
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
        }
        
        // Bytes written
        if stat.avgBytesWritten.Valid {
            writtenMetric := scopeMetrics.Metrics().AppendEmpty()
            writtenMetric.SetName("snowflake.query.bytes_written")
            writtenMetric.SetDescription("Average bytes written per query")
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
        }
        
        // Rows produced
        if stat.avgRowsProduced.Valid {
            rowsMetric := scopeMetrics.Metrics().AppendEmpty()
            rowsMetric.SetName("snowflake.query.rows_produced")
            rowsMetric.SetDescription("Average rows produced per query")
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
        }
        
        // Compilation time
        if stat.avgCompilationTime.Valid {
            compMetric := scopeMetrics.Metrics().AppendEmpty()
            compMetric.SetName("snowflake.query.compilation_time")
            compMetric.SetDescription("Average query compilation time")
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
        }
    }
}
